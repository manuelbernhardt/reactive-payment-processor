package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.bernhardt.reactivepayment.PaymentProcessor.{Order, OrderIdentifier}

/**
  * Handles a single order from a specific client
  */
class OrderHandler(order: Order, client: ActorRef, orderStorage: ActorRef, validator: ActorRef) extends Actor with ActorLogging {

  import OrderHandler._

  val orderIdentifier = OrderIdentifier.generate

  context.system.eventStream.subscribe(self, classOf[OrderStorage.OrdersChanged])

  orderStorage ! OrderStorage.RegisterOrder(orderIdentifier, order, self)

  log.info("Received new order {}", orderIdentifier)

  def receive: Receive = {
    case OrderStorage.OrderRegistered(id, order) =>
      log.info("Order {} registered in storage", orderIdentifier)
      validator ! Validator.ValidateOrder(id, order)
    case Validator.OrderValidated(id, order, bankIdentifier) =>
      log.info("Order {} validated", orderIdentifier)
      orderStorage ! OrderStorage.StoreOrderValidation(id, order, bankIdentifier, self)
    case Validator.OrderRejected(id, order) =>
      log.warning("Order {} failed validation - rejected", orderIdentifier)
      orderStorage ! OrderStorage.StoreOrderRejection(id, order)
      orderStorage ! OrderStorage.StoreOrderDone(orderIdentifier, order)
      client ! OrderRejected(id)
    case OrderStorage.OrderValidationStored(id, _) =>
      log.info("Order {} validation stored", id)

    case OrderStorage.OrdersChanged(orders) if orders.contains(orderIdentifier.i.toString) =>
      val order = orders.get(orderIdentifier.i.toString)
      order.foreach { order =>
        order.status match {
          case OrderStatus.Executed =>
            client ! OrderExecuted(orderIdentifier)
            orderStorage ! OrderStorage.StoreOrderDone(orderIdentifier, order.order)
          case OrderStatus.Failed =>
            client ! OrderFailed(orderIdentifier)
            orderStorage ! OrderStorage.StoreOrderDone(orderIdentifier, order.order)
          case _ =>
          // ignore any of these
        }
      }
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
  }

}

object OrderHandler {

  def props(order: Order, client: ActorRef, orderStorage: ActorRef, validator: ActorRef): Props =
    Props(new OrderHandler(order, client, orderStorage, validator))

  case class OrderCreated(id: OrderIdentifier)
  case class OrderRejected(id: OrderIdentifier)
  case class OrderExecuted(id: OrderIdentifier)
  case class OrderFailed(id: OrderIdentifier)
}
