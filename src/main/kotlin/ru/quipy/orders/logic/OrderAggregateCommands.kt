package ru.quipy.orders.logic

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker
import ru.quipy.domain.Event
import ru.quipy.orders.api.*
import java.util.*


@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")

fun OrderAggregateState.create(id: UUID, userId: UUID): OrderCreatedEvent {
    return OrderCreatedEvent(
        orderId = id,
        userId = userId,
    )
}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")

fun OrderAggregateState.addItem(productId: UUID, amount: Int): List<Event<OrderAggregate>> {
    val itemAddedEvent = ItemAddedEvent(
        orderId = this.getId(),
        item = CartItem(productId, amount)
    )

    return when (this.status) {
        OrderStatus.COLLECTING -> listOf(itemAddedEvent)
        OrderStatus.BOOKING_IN_PROGRESS, OrderStatus.BOOKED, OrderStatus.DELIVERY_SET -> {
            listOf(CheckoutCancelledEvent(this.getId(), bookingId!!), itemAddedEvent)
        }
        else -> error("Order ${getId()} is in status $status, cannot add item")
    }

}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")

fun OrderAggregateState.checkout(): OrderCheckoutStartedEvent {
    if (status != OrderStatus.COLLECTING)
        error("Cannot start checkout for order ${getId()}, status is $status")

    return OrderCheckoutStartedEvent(
        orderId = this.getId(),
        bookingId = UUID.randomUUID(),
        items = this.shoppingCart.items
    )
}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")

fun OrderAggregateState.setBookingResults(bookingId: UUID, success: Boolean, totalPrice: Int? = null): OrderBookingResultSetEvent {
    if (status != OrderStatus.BOOKING_IN_PROGRESS)
        error("Cannot set booking results for order ${getId()}, status is $status")

    return OrderBookingResultSetEvent(
        orderId = this.getId(),
        bookingId = bookingId,
        success = success,
        totalPrice = totalPrice,
    )
}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")
fun OrderAggregateState.setDelivery(deliveryId: UUID): OrderDeliverySetEvent {
    if (status != OrderStatus.BOOKED && status != OrderStatus.DELIVERY_SET)
        error("Cannot set delivery for order ${getId()}, status is $status")

    return OrderDeliverySetEvent(
        orderId = this.getId(),
        deliveryId = deliveryId,
    )
}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")
fun OrderAggregateState.setPaymentResults(
    paymentId: UUID,
    success: Boolean,
    txId: UUID? = null,
    reason: String? = null
): Event<OrderAggregate> {
    if (status != OrderStatus.PAYMENT_IN_PROGRESS)
        error("Cannot set booking results for order ${getId()}, status is $status")

    return if (success) {
        OrderPaymentSucceededEvent(
            this.getId(),
            paymentId
        )
    } else {
        OrderPaymentFailedEvent(
            this.getId(),
            paymentId,
            txId,
            reason
        )
    }
}

@CircuitBreaker(name = "confirmPayment2", fallbackMethod = "pendingAuthorizedPaymentIntegration")
fun OrderAggregateState.startPayment(): OrderPaymentStartedEvent {
    return when (this.status) {

        OrderStatus.BOOKED, OrderStatus.DELIVERY_SET, OrderStatus.PAYMENT_FAILED -> {
            if (bookingId != null) {
                OrderPaymentStartedEvent(
                    orderId = this.getId(),
                    paymentId = UUID.randomUUID(),
                    this.priceToPay!!,
                )
            } else {
                error("Booking is not success or delivery is not set")
            }
        }
        else -> error("Order is in status ${this.status}, cannot start payment")
    }
}

fun pendingAuthorizedPaymentIntegration(id: Long, throwable: Throwable) {
    println("ahaahahah")
}
