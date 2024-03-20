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

    private val paymentListener = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-listener"))
    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))
    private val serversSla = ConcurrentHashMap<PaymentService, Float>()
    private val serversDuration = ConcurrentHashMap<PaymentService, ConcurrentLinkedQueue<Float>>()
    private val paymentServer = ConcurrentHashMap<UUID, PaymentService>()

    @PostConstruct
    fun init() {
        serversSla[paymentServiceDefault] = 0f
        serversSla[paymentServiceOptional] = 0f
        serversDuration[paymentServiceDefault] = ConcurrentLinkedQueue<Float>()
        serversDuration[paymentServiceOptional] = ConcurrentLinkedQueue<Float>()
    }

    fun paymentServerCall(paymentId: UUID, orderId: UUID, amount: Int, createdAt: Long) {
        if (/*serversSla[paymentServiceDefault]!! < 5f*/ serversDuration[paymentServiceDefault]!!.size == 0 || serversDuration[paymentServiceDefault]!!.sum() / serversDuration[paymentServiceDefault]!!.size < 4f) {
            paymentServer[paymentId] = paymentServiceDefault
        } else {
            if (serversSla[paymentServiceOptional]!! < 80f) {
                paymentServer[paymentId] = paymentServiceOptional
            }
        }

        if (paymentServer.containsKey(paymentId)) {
            try {
                paymentServer[paymentId]!!.submitPaymentRequest(paymentId, amount, createdAt)
            } catch (e: Exception) {
                if (paymentServer[paymentId] == paymentServiceDefault) {
                    paymentServer[paymentId] = paymentServiceOptional
                    paymentServer[paymentId]!!.submitPaymentRequest(paymentId, amount, createdAt)
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

    fun getPaymentInfo(paymentId: UUID, orderId: UUID, amount: Int, processedAt: Long, submittedAt: Long, success: Boolean) {
        paymentListener.submit {
            var duration = (processedAt - submittedAt) / 1000f
            if (!success) {
                duration = 80f
            }
            var sla = serversSla[paymentServer[paymentId]!!]!!
            val queue = serversDuration[paymentServer[paymentId]!!]
            if (queue!!.count() < 100) {
                queue.add(duration)
                sla += duration / 100f
            } else {
                val elem = queue.first()
                queue.remove(elem)
                queue.add(duration)
                sla -= elem / 100f
                sla += duration / 100f
            }

            if (paymentServer[paymentId]!! != paymentServiceDefault /*&& serversSla[paymentServiceDefault]!! > 5f*/) {
                val queueOther = serversDuration[paymentServiceDefault]!!
                val elem = queueOther.first()
                queueOther.remove(elem)
                serversSla[paymentServiceDefault] = serversSla[paymentServiceDefault]!! - elem / 100f
            }

            logger.info("Payment $paymentId for order $orderId succeeded.")
            serversSla[paymentServer[paymentId]!!] = sla


            System.out.println("")
            System.out.println(sla)
            System.out.println("")
        }
    }
}