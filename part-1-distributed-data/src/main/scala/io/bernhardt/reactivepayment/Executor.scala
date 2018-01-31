package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.{DistributedData, Replicator}
import io.bernhardt.reactivepayment.OrderStorage.Key
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, OrderIdentifier}


/**
  * Responsible for executing orders, which is to say communicate with the acquiring bank
  * In this example, the communication with an acquiring bank is restricted to certain cluster nodes
  * that have the required hardware token to communicate with the bank. This mechanism is represented by
  * cluster roles.
  */
class Executor(orderStorage: ActorRef) extends Actor with ActorLogging {

  val replicator = DistributedData(context.system).replicator

  implicit val cluster = Cluster(context.system)

  val supportedBanks: Set[String] =
    cluster
      .selfRoles
      .filter(_.startsWith(BankPrefix))
      .map(_.drop(BankPrefix.length))

  supportedBanks.foreach(b => context.actorOf(BankConnection.props(BankIdentifier(b)), b))

  context.system.eventStream.subscribe(self, classOf[OrderStorage.OrdersChanged])

  def receive: Receive = {
    case OrderStorage.OrdersChanged(orders) =>
      val relevantOrders = orders.values.filter { v =>
        v.status == OrderStatus.Validated && v.bankIdentifier.isDefined && supportedBanks(v.bankIdentifier.get.b)
      }
      val groupedOrders = relevantOrders.groupBy(_.bankIdentifier.get)
      executeOrders(groupedOrders)
    case BankConnection.OrderExecutionSucceeded(id, order) =>
      log.info("Successful execution of order {}", id)
      orderStorage ! OrderStorage.StoreOrderExecuted(id, order)
    case Replicator.UpdateSuccess(Key, Some(id: OrderIdentifier)) =>
      log.info("Order {} updated successfully", id)
  }

  private def executeOrders(groupedOrders: Map[BankIdentifier, Iterable[StoredOrder]]): Unit = {
    groupedOrders.foreach { case (bank, orders) =>
      orders.foreach { order =>
        context.child(bank.b) match {
          case Some(connection) =>
            connection ! BankConnection.ExecuteOrder(order.id, order.order)
          case None =>
            log.error("Cannot execute order for unknown bank {}", bank)
            orderStorage ! OrderStorage.StoreOrderFailed(order.id, order.order)
        }
      }
    }
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
  }
}

object Executor {

  def props(storage: ActorRef) = Props(new Executor(storage))

}
