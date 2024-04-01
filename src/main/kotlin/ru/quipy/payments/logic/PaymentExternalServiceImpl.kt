package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeoutException
import javax.annotation.PostConstruct
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        //private val paymentsProcessDispatcher = Executors.newFixedThreadPool(80)
        private val paymentsProcessDispatcher = Executors.newFixedThreadPool(80, NamedThreadFactory("payment-process-dispatcher"))
        val paymentOperationTimeout = Duration.ofSeconds(80)
        val protocols = listOf(Protocol.H2_PRIOR_KNOWLEDGE)
        val dispatcher = Dispatcher(paymentsProcessDispatcher).apply {
            maxRequests = 10000
            maxRequestsPerHost = 10000
        }

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val speed = min(parallelRequests.toFloat() / requestAverageProcessingTime.seconds.toFloat(), rateLimitPerSec.toFloat())

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private var paymentBalancer: PaymentExternalServiceBalancer? = null

    private val paymentsQueue: BlockingQueue<Runnable> = LinkedBlockingQueue((80*speed).toInt()+1)

    private val paymentsProcessExecutor = Executors.newFixedThreadPool(5, NamedThreadFactory("payment-process-executor"))
    private val paymentsResultExecutor = Executors.newFixedThreadPool(5, NamedThreadFactory("payment-result-executor"))
    private val paymentsRunnerExecutor = Executors.newFixedThreadPool(5, NamedThreadFactory("payment-runner-executor"))
    //private val paymentsProcessExecutor = Executors.newFixedThreadPool(5)
    //private val paymentsResultExecutor = Executors.newFixedThreadPool(5)
    //private val paymentsRunnerExecutor = Executors.newFixedThreadPool(5)

    private var client = OkHttpClient.Builder()
            .callTimeout(paymentOperationTimeout)
            .protocols(protocols)
            .dispatcher(dispatcher)
            .build()

    private val rateLimiter = RateLimiter(rateLimitPerSec);
    private val window = OngoingWindow(parallelRequests);
    private fun setDispatcherParameters(dispatcher: Dispatcher): Dispatcher {
        dispatcher.maxRequests = 10000
        dispatcher.maxRequestsPerHost = 10000
        return dispatcher
    }
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()
        if ((Duration.ofMillis(now() - paymentStartedAt) + requestAverageProcessingTime).seconds > 80) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }
        val startTime = now()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                window.release()

                paymentsResultExecutor.submit {
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }

            }

            override fun onResponse(call: Call, response: Response) {
                window.release()

                paymentsResultExecutor.submit {
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        })
    }

    override fun getAverageProcessingTime(): Float {
        return requestAverageProcessingTime.toMillis() / 1000f
    }

    override fun getRateLimitPerSec(): Int {
        return rateLimitPerSec
    }

    override fun getParallelRequests(): Int {
        return parallelRequests
    }

    override fun getBalancer(): PaymentExternalServiceBalancer {
        return paymentBalancer!!
    }

    override fun setBalancer(balancer: PaymentExternalServiceBalancer) {
        paymentBalancer = balancer
    }

    override fun processPayments() {
        while (true) {
            window.acquire()
            rateLimiter.tickBlocking()

            val task = paymentsQueue.take()
            paymentsProcessExecutor.execute(task)
        }
    }

    override fun runProcesses() {
        paymentsRunnerExecutor.submit {
            processPayments()
        }
    }

    override fun addToQueue(task: Runnable) {
        paymentsQueue.put(task)
    }

    override fun getQueueSize(): Int {
        return paymentsQueue.size
    }

    override fun getSpeed(): Float {
        return speed
    }
}

public fun now() = System.currentTimeMillis()
