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
    private lateinit var paymentServicePrimary: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.SECONDARY_PAYMENT_BEAN)
    private lateinit var paymentServiceSecondary: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.TERTIARY_PAYMENT_BEAN)
    private lateinit var paymentServiceTertiary: PaymentService

//    @Autowired
//    @Qualifier(ExternalServicesConfig.FOURTH_PAYMENT_BEAN)
//    private lateinit var paymentServiceFourth: PaymentService
//
//    @Autowired
//    @Qualifier(ExternalServicesConfig.FIFTH_PAYMENT_BEAN)
//    private lateinit var paymentServiceFifth: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))
    private val circuitBreaker = CircuitBreaker(failureThreshold = 5, recoveryTimeout = Duration.ofMinutes(1))

    val services: List<PaymentService>
        get() = listOf(paymentServicePrimary, paymentServiceSecondary, paymentServiceTertiary)

    @PostConstruct
    fun init() {
        services.forEach { service ->
            service.runProcesses()
        }
    }

    fun isFastEnough(server: PaymentService, timePassed: Float) : Boolean {
        return (server.getQueueSize() + 1) / server.getSpeed() + timePassed < 80
    }

    fun paymentServerCall(paymentId: UUID, orderId: UUID, amount: Int, createdAt: Long) {
        val timePassed = (System.currentTimeMillis() - createdAt) / 1000f

        var isSubmitted = false
        for (service in services) {
            if (isFastEnough(service, timePassed)) {
                service.addToQueue(Runnable {
                    try {
                        service.submitPaymentRequest(paymentId, amount, createdAt)
                    } catch (e: Exception) {
                        logger.error("Произошла ошибка при отправке запроса на оплату", e)
                    }
                })
                isSubmitted = true
                break
            }
        }

        if (!isSubmitted) {
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
class CircuitBreaker(private val failureThreshold: Int, private val recoveryTimeout: Duration) {
    private var failureCount = 0
    private var lastFailureTimestamp: Long = 0

    fun recordSuccess() {
        reset()
    }

    fun recordFailure() {
        if (isRecoveryTimeoutExpired()) {
            reset()
        }
        failureCount++
        lastFailureTimestamp = System.currentTimeMillis()
    }

    fun isCircuitOpen(): Boolean {
        return failureCount >= failureThreshold && isRecoveryTimeoutExpired()
    }

    private fun reset() {
        failureCount = 0
        lastFailureTimestamp = 0
    }

    private fun isRecoveryTimeoutExpired(): Boolean {
        return System.currentTimeMillis() - lastFailureTimestamp >= recoveryTimeout.toMillis()
    }
}