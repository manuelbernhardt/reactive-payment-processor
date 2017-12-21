package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.{DistributedData, Replicator}
import io.bernhardt.reactivepayment.PaymentProcessor.{Order, OrderIdentifier}

/**
  * Handles a single order from a specific client
  */
class ClientOrderHandler(order: Order, client: ActorRef, orderStorage: ActorRef, validator: ActorRef) extends Actor with ActorLogging {

  import ClientOrderHandler._

  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)

  replicator ! Replicator.Subscribe(OrderStorage.Key, self)

  val orderIdentifier = OrderIdentifier.generate


  orderStorage ! OrderStorage.RegisterOrder(orderIdentifier, order, self)
  // TODO handle failure thereof

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
      client ! OrderRejected(id)
    case OrderStorage.OrderValidationStored(id, _) =>
      log.info("Order {} validation stored", id)

    case change @ Replicator.Changed(OrderStorage.Key) =>
      val allOrders = change.get(OrderStorage.Key)
      val order = allOrders.entries.get(orderIdentifier.i.toString)
      order.foreach { order =>
        order.status match {
          case OrderStatus.Executed =>
            client ! OrderExecuted(orderIdentifier)
          case OrderStatus.Failed =>
            client ! OrderFailed(orderIdentifier)
          case _ =>
          // we don't care about these
        }
      }


  }

}

object ClientOrderHandler {

  def props(order: Order, client: ActorRef, orderStorage: ActorRef, validator: ActorRef): Props =
    Props(new ClientOrderHandler(order, client, orderStorage, validator))

  case class OrderCreated(id: OrderIdentifier)
  case class OrderRejected(id: OrderIdentifier)
  case class OrderExecuted(id: OrderIdentifier)
  case class OrderFailed(id: OrderIdentifier)
}
