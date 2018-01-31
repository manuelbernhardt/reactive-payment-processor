package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, Props, Timers}
import io.bernhardt.reactivepayment.PaymentProcessor.{Order, OrderIdentifier, _}
import java.time._
import scala.concurrent.duration._

// TODO make persistent
class Executor extends Actor with ActorLogging with Timers {

  import Executor._

  timers.startPeriodicTimer(ReceivedOrderCleanup, ReceivedOrderCleanup, 1.day)

  var receivedOrders = Map.empty[OrderIdentifier, LocalDateTime]

  // TODO this is of course not adequately routed yet
  val bankConnection = context.actorOf(BankConnection.props(BankA))

  def receive: Receive = {
    case ExecuteOrder(id, order, deliveryId) =>
      if (!receivedOrders.contains(id)) {
        receivedOrders += id -> LocalDateTime.now
        bankConnection ! BankConnection.ExecuteOrder(id, order, sender())
      }
      sender() ! Merchant.ConfirmExecuteOrderDelivery(deliveryId, order)
    case BankConnection.OrderExecutionSucceeded(id, order, replyTo) =>
      replyTo ! Merchant.ProcessOrderExecuted(id, order)
    case BankConnection.OrderExecutionFailed(id, order, replyTo) =>
      replyTo ! Merchant.ProcessOrderFailed(id, order)
    case ReceivedOrderCleanup =>
      receivedOrders = receivedOrders.filterNot { case (_, added) =>
        added.isBefore(LocalDateTime.now.minus(Period.ofDays(1)))
      }
  }

}

object Executor {
  def props() = Props(new Executor)

  sealed trait Command

  case class ExecuteOrder(id: OrderIdentifier, order: Order, deliveryId: Long) extends Command

  sealed trait Event
  case class OrderScheduledForExecution(id: OrderIdentifier, order: Order) extends Event

  private case object ReceivedOrderCleanup
}
