package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.subscribers.OrderPaymentSubscriber
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.annotation.PostConstruct

@Service
class PaymentExternalServiceBalancer {
    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    @Qualifier(ExternalServicesConfig.PRIMARY_PAYMENT_BEAN)
    private lateinit var paymentServiceDefault: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.OPTIONAL_PAYMENT_BEAN)
    private lateinit var paymentServiceOptional: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))
    private val paymentServer = ConcurrentHashMap<UUID, PaymentService>()

    @PostConstruct
    fun init() {
        paymentServiceDefault.setBalancer(this)
        paymentServiceOptional.setBalancer(this)
    }

    fun isFastEnough(server: PaymentService, timePassed: Float) : Boolean {
        return server.getAverageProcessingTime() + timePassed < 80
        /*if (serversDuration[server]!!.size == 0) {
            return true
        }
        return serversDuration[server]!!.sum() / serversDuration[server]!!.size  < server.getAverageProcessingTime()*/
    }

    fun paymentServerCall(paymentId: UUID, orderId: UUID, amount: Int, createdAt: Long) {
        val timePassed = (System.currentTimeMillis() - createdAt) / 1000f
        if (isFastEnough(paymentServiceDefault, timePassed)) {
            paymentServer[paymentId] = paymentServiceOptional
        } else {
            if (isFastEnough(paymentServiceOptional, timePassed)) {
                paymentServer[paymentId] = paymentServiceOptional
            }
        }

        if (paymentServer.containsKey(paymentId)) {
            try {
                paymentServer[paymentId]!!.submitPaymentRequest(paymentId, amount, createdAt)
            } catch (e: Exception) {
                if (paymentServer[paymentId] == paymentServiceDefault) {
                    if (isFastEnough(paymentServiceOptional, timePassed)) {
                        paymentServer[paymentId] = paymentServiceOptional
                        paymentServer[paymentId]!!.submitPaymentRequest(paymentId, amount, createdAt)
                    } else {
                        paymentServer.remove(paymentId)
                    }
                }
            }
        } else {
            val transactionId = UUID.randomUUID()
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - createdAt))
            }
            val cancelledEvent = paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request canceled. No free services")
            }
            logger.info("Payment $paymentId for order $orderId created.")
        }
    }

    fun processPayment(paymentId: UUID, orderId: UUID, amount: Int, createdAt: Long) {
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.info("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentServerCall(paymentId, orderId, amount, createdAt)
        }
    }
}