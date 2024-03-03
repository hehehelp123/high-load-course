package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.*
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct
import kotlin.collections.HashMap

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    @Qualifier(ExternalServicesConfig.PRIMARY_PAYMENT_BEAN)
    private lateinit var paymentServiceDefault: PaymentService

    @Autowired
    @Qualifier(ExternalServicesConfig.OPTIONAL_PAYMENT_BEAN)
    private lateinit var paymentServiceOptional: PaymentService

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))
    private val serversSla = HashMap<PaymentService, Float>()
    private val paymentServer = HashMap<UUID, PaymentService>()
    private val serverPaymentCount = HashMap<PaymentService, Int>()

    @PostConstruct
    fun init() {
        serversSla[paymentServiceDefault] = 0f
        serversSla[paymentServiceOptional] = 0f
        serverPaymentCount[paymentServiceDefault] = 0
        serverPaymentCount[paymentServiceOptional] = 0
        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount
                        )
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    if (serversSla[paymentServiceDefault]!! < 5f || serverPaymentCount[paymentServiceDefault]!! < 100) {
                        paymentServer[createdEvent.paymentId] = paymentServiceDefault
                    } else {
                        if (serversSla[paymentServiceOptional]!! < 80f || serverPaymentCount[paymentServiceDefault]!! < 1000) {
                            paymentServer[createdEvent.paymentId] = paymentServiceOptional
                        }
                    }

                    if (paymentServer.containsKey(createdEvent.paymentId)) {
                        paymentServer[createdEvent.paymentId]!!.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                        serverPaymentCount[paymentServer[createdEvent.paymentId]!!] = serverPaymentCount[paymentServer[createdEvent.paymentId]]!! + 1
                    } else {
                        val transactionId = UUID.randomUUID()
                        paymentESService.update(createdEvent.paymentId) {
                            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - createdEvent.createdAt))
                        }
                        val cancelledEvent =  paymentESService.update(createdEvent.paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request canceled. No free services")
                        }
                        logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")
                    }
                }
            }
        }

        subscriptionsManager.createSubscriber(PaymentAggregate::class, "payments:payment-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(PaymentProcessedEvent::class) { event ->
                paymentExecutor.submit {
                    val sla = (event.processedAt - event.submittedAt) / 1000f
                    logger.info("Payment ${event.paymentId} for order ${event.orderId} succeeded.")
                    serversSla[paymentServer[event.paymentId]!!] = sla
                    serverPaymentCount[paymentServer[event.paymentId]!!] = serverPaymentCount[paymentServer[event.paymentId]]!! - 1
                }
            }
        }
    }
}