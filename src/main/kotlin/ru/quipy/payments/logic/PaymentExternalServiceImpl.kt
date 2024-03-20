package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val speed = min(parallelRequests / requestAverageProcessingTime.seconds.toInt(), rateLimitPerSec)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private var paymentBalancer: PaymentExternalServiceBalancer? = null

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val rateLimiter = RateLimiter(rateLimitPerSec);
    private val window = NonBlockingOngoingWindow(parallelRequests);

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        rateLimiter.tick()
        window.putIntoWindow()
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

        val startTime = now()
        OkHttpClient.Builder().callTimeout(paymentOperationTimeout - Duration.ofMillis(now() - paymentStartedAt)).run {
            build()
        }.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                window.releaseWindow()
                paymentBalancer!!.getPaymentInfo(paymentId, startTime, now())
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

            override fun onResponse(call: Call, response: Response) {
                window.releaseWindow()
                paymentBalancer!!.getPaymentInfo(paymentId, startTime, now())
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
}

public fun now() = System.currentTimeMillis()
