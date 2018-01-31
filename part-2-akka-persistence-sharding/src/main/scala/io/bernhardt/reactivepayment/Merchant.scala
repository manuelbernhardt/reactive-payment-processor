package io.bernhardt.reactivepayment

import akka.actor.{ActorLogging, ActorRef, Props}
import akka.persistence.{AtLeastOnceDelivery, PersistentActor}
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}

class Merchant(validator: ActorRef, executor: ActorRef) extends PersistentActor with AtLeastOnceDelivery with ActorLogging {

  import Merchant._

  override def persistenceId = s"Merchant-${self.path.name}"

  var orderClients = Map.empty[OrderIdentifier, ActorRef]
  var orders = Map.empty[OrderIdentifier, StoredOrder]

  override def receiveCommand: Receive = {
    case ProcessNewOrder(order) =>
      log.info("Received new order for processing")
      persist(OrderCreated(StoredOrder(OrderIdentifier.generate, OrderStatus.New, order, None)))(handleEvent)
    case ProcessOrderValidated(id, _, bankIdentifier) =>
      log.info("Order {} validated", id)
      persist(OrderValidated(id, bankIdentifier))(handleEvent)
    case ProcessOrderRejected(id, _) =>
      log.info("Order {} rejected", id)
      persist(OrderRejected(id))(handleEvent)
    case ConfirmExecuteOrderDelivery(deliveryId, _) =>
      log.info("Order execution delivery confirmed")
      confirmDelivery(deliveryId)
    case ProcessOrderExecuted(id, _) =>
      log.info("Order {} executed", id)
      persist(OrderExecuted(id))(handleEvent)
    case ProcessOrderFailed(id, _) =>
      log.info("Order {} failed", id)
      persist(OrderFailed(id))(handleEvent)
  }

  override def receiveRecover: Receive = {
    case event: MerchantEvent => handleEvent(event)
  }

  private def handleEvent(event: MerchantEvent): Unit = event match {
    case OrderCreated(order) =>
      log.info("Order created")
      orders += order.id -> order
      if (recoveryFinished) {
        orderClients += order.id -> sender()
        validator ! Validator.ValidateOrder(order.id, order.order)
      }
    case OrderValidated(id, bankIdentifier) =>
      updateOrder(id) { storedOrder =>
        storedOrder.copy(status = OrderStatus.Validated, bankIdentifier = Some(bankIdentifier))
      }
      orders.get(id) match {
        case Some(storedOrder) =>
          deliver(executor.path) { deliveryId =>
            Executor.ExecuteOrder(id, storedOrder.order, deliveryId)
          }
        case None =>
          log.warning("Could not find validated order to execute")
      }
    case OrderRejected(id) =>
      updateOrder(id) { storedOrder =>
        storedOrder.copy(status = OrderStatus.Rejected)
      }
      if(recoveryFinished) {
        // in real life we'd need with the client not being found / we'd need to signal back to it differently
        orderClients.get(id).foreach { client =>
          client ! OrderRejected(id)
        }
      }
    case OrderExecuted(id) =>
      updateOrder(id) { storedOrder =>
        storedOrder.copy(status = OrderStatus.Executed)
      }
      if (recoveryFinished) {
        orderClients.get(id).foreach { client =>
          client ! OrderExecuted(id)
        }
      }
    case OrderFailed(id) =>
      updateOrder(id) { storedOrder =>
        storedOrder.copy(status = OrderStatus.Failed)
      }
      if(recoveryFinished) {
        orderClients.get(id).foreach { client =>
          client ! OrderFailed(id)
        }
      }
  }


  private def updateOrder(id: OrderIdentifier)(update: StoredOrder => StoredOrder): Unit = {
    orders.get(id) match {
      case Some(storedOrder) =>
        orders += id -> update(storedOrder)
      case None =>
        log.warning("Could not update unkown order {}", id)
    }
  }


}

object Merchant {

  def props(validator: ActorRef, executor: ActorRef): Props = Props(new Merchant(validator, executor))

  sealed trait MerchantCommand {
    val order: Order
  }
  case class ProcessNewOrder(order: Order) extends MerchantCommand
  case class ProcessOrderValidated(id: OrderIdentifier, order: Order, bankIdentifier: BankIdentifier) extends MerchantCommand
  case class ProcessOrderRejected(id: OrderIdentifier, order: Order) extends MerchantCommand
  case class ConfirmExecuteOrderDelivery(deliveryId: Long, order: Order) extends MerchantCommand
  case class ProcessOrderExecuted(id: OrderIdentifier, order: Order) extends MerchantCommand
  case class ProcessOrderFailed(id: OrderIdentifier, order: Order) extends MerchantCommand

  sealed trait MerchantEvent
  case class OrderCreated(order: StoredOrder) extends MerchantEvent
  case class OrderValidated(id: OrderIdentifier, bankIdentifier: BankIdentifier) extends MerchantEvent
  case class OrderRejected(id: OrderIdentifier) extends MerchantEvent
  case class OrderExecuted(id: OrderIdentifier) extends MerchantEvent
  case class OrderFailed(id: OrderIdentifier) extends MerchantEvent

  case class StoredOrder(id: OrderIdentifier, status: OrderStatus, order: Order, bankIdentifier: Option[BankIdentifier])

}
