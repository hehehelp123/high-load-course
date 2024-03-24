package ru.quipy.payments.logic

import java.time.Duration
import java.util.*

interface PaymentService {
    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)

    fun getAverageProcessingTime() : Float

    fun getParallelRequests(): Int
    fun getRateLimitPerSec(): Int
    fun getBalancer(): PaymentExternalServiceBalancer
    fun setBalancer(balancer: PaymentExternalServiceBalancer)
    fun processPayments()
    fun runProcesses()
    fun addToQueue(task: Runnable)
    fun getQueueSize(): Int
    fun getSpeed(): Float
}

interface PaymentExternalService : PaymentService

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11)
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)