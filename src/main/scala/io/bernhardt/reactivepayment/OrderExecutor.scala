package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.{DistributedData, ORMap, Replicator}
import io.bernhardt.reactivepayment.OrderStorage.Key
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}

import scala.concurrent.duration._

/**
  * Responsible for executing orders, which is to say communicate with the acquiring bank
  * In this example, the communication with an acquiring bank is restricted to certain cluster nodes
  * that have the required hardware token to communicate with the bank. This mechanism is represented by
  * cluster roles.
  */
class OrderExecutor extends Actor with ActorLogging {

  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)

  val supportedBanks: Set[String] =
    cluster
      .selfRoles
      .filter(_.startsWith(BankPrefix))
      .map(_.drop(BankPrefix.length))

  supportedBanks.foreach(b => context.actorOf(BankConnection.props(BankIdentifier(b)), b))

  replicator ! Replicator.Subscribe(OrderStorage.Key, self)

  def receive: Receive = {
    case change@Replicator.Changed(OrderStorage.Key) =>
      // in reality, this should be optimized by storing executable orders for a bank in a separate ORMap dedicated to this bank
      val allOrders = change.get(OrderStorage.Key)
      log.info(allOrders.entries.toString())
      val relevantOrders = allOrders.entries.values.filter { v =>
        v.status == OrderStatus.Validated && v.bankIdentifier.isDefined && supportedBanks(v.bankIdentifier.get.b)
      }
      log.info(relevantOrders.toString())
      val groupedOrders = relevantOrders.groupBy(_.bankIdentifier.get)
      executeOrders(groupedOrders)
    case BankConnection.OrderExecutionSucceeded(id, order) =>
      log.info("Successful execution of order {}", id)
      storeExecutionResult(id, order, OrderStatus.Executed)
    case Replicator.UpdateSuccess(Key, Some(id: OrderIdentifier)) =>
      // happy

    // TODO handle storage failures
  }

  private def executeOrders(groupedOrders: Map[BankIdentifier, Iterable[StoredOrder]]): Unit = {
    groupedOrders.foreach { case (bank, orders) =>
      orders.foreach { order =>
        context.child(bank.b) match {
          case Some(connection) =>
            connection ! BankConnection.ExecuteOrder(order.id, order.order)
          case None =>
            log.error("Cannot execute order for unknown bank {}", bank)
            storeExecutionResult(order.id, order.order, OrderStatus.Failed)
        }
      }
    }
  }

  private def storeExecutionResult(id: OrderIdentifier, order: Order, status: OrderStatus): Unit = {
    val storedOrder = StoredOrder(id, status, order, None)
    replicator ! Replicator.Update(Key, ORMap.empty[String, StoredOrder], Replicator.WriteMajority(5.seconds), Some(id)) { orders =>
      orders + (id.i.toString -> storedOrder)
    }
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    reason.printStackTrace()
    super.preRestart(reason, message)
  }

}

object OrderExecutor {

  def props() = Props(new OrderExecutor)

}
