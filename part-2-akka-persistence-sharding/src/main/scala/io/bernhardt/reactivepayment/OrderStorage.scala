package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.ddata._
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}

import scala.concurrent.duration._

/**
  * Stores orders using Akka's Distributed Data module for master to master replication.
  * Orders themselves are immutable and only their state as well as some auxiliary validation data can change over time,
  * which is to say that they can be quite easily represented as custom CRDTs (see [[ io.bernhardt.reactivepayment.StoredOrder ]]
  * and [[ io.bernhardt.reactivepayment.OrderStatus ]]).
  */
class OrderStorage extends Actor with ActorLogging {

  import OrderStorage._

  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)

  replicator ! Replicator.Subscribe(OrderStorage.Key, self)


  def receive = {
    case request @ RegisterOrder(id, order, _) =>
      val storedOrder = OStoredOrder(id, OrderStatus.New, order, None)
      replicator ! Replicator.Update(Key, ORMap.empty[String, OStoredOrder], Replicator.WriteLocal, Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }
    case Replicator.UpdateSuccess(Key, Some(request: RegisterOrder)) =>
      request.replyTo ! OrderRegistered(request.id, request.order)

    case request @ StoreOrderValidation(id, order, bankIdentifier, _) =>
      val storedOrder = OStoredOrder(id, OrderStatus.Validated, order, Some(bankIdentifier))
      replicator ! Replicator.Update(
        key = Key,
        initial = ORMap.empty[String, OStoredOrder],
        writeConsistency = Replicator.WriteMajority(5.seconds),
        request = Some(request)
      ) { orders =>
        orders + (id.i.toString -> storedOrder)
      }
    case Replicator.UpdateSuccess(Key, Some(request: StoreOrderValidation)) =>
      request.replyTo ! OrderValidationStored(request.id, request.order)

    case request @ StoreOrderRejection(id, order) =>
      val storedOrder = OStoredOrder(id, OrderStatus.Rejected, order, None)
      replicator ! Replicator.Update(Key, ORMap.empty[String, OStoredOrder], Replicator.WriteMajority(5.seconds), Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }

    case request @ StoreOrderExecuted(id, order) =>
      storeExecutionResult(id, order, OrderStatus.Executed, request)

    case request @ StoreOrderFailed(id, order) =>
      storeExecutionResult(id, order, OrderStatus.Failed, request)

    case request @ StoreOrderDone(id, order) =>
      val storedOrder = OStoredOrder(id, OrderStatus.Done, order, None)
      replicator ! Replicator.Update(Key, ORMap.empty[String, OStoredOrder], Replicator.WriteMajority(5.seconds), Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }

    case Replicator.UpdateSuccess(Key, Some(request: StorageCommand)) =>
      log.info("Order {} updated successfully", request.id)

    case change @ Replicator.Changed(OrderStorage.Key) =>
      val allOrders = change.get(OrderStorage.Key).entries
      context.system.eventStream.publish(OrderStorage.OrdersChanged(allOrders))

  }

  private def storeExecutionResult(id: OrderIdentifier, order: Order, status: OrderStatus, request: Any): Unit = {
    val storedOrder = OStoredOrder(id, status, order, None)
    replicator ! Replicator.Update(Key, ORMap.empty[String, OStoredOrder], Replicator.WriteMajority(5.seconds), Some(request)) { orders =>
      orders + (id.i.toString -> storedOrder)
    }
  }


}

object OrderStorage {

  def props() = Props(new OrderStorage())

  val Key = ORMapKey.create[String, OStoredOrder]("orders")

  case class OrdersChanged(orders: Map[String, OStoredOrder])

  sealed trait StorageCommand {
    val id: OrderIdentifier
  }

  case class RegisterOrder(id: OrderIdentifier, order: Order, replyTo: ActorRef) extends StorageCommand
  case class OrderRegistered(id: OrderIdentifier, order: Order)

  case class StoreOrderValidation(id: OrderIdentifier, order: Order, bankIdentifier: BankIdentifier, replyTo: ActorRef) extends StorageCommand
  case class OrderValidationStored(id: OrderIdentifier, order: Order)

  case class StoreOrderRejection(id: OrderIdentifier, order: Order) extends StorageCommand

  case class StoreOrderExecuted(id: OrderIdentifier, order: Order) extends StorageCommand
  case class StoreOrderFailed(id: OrderIdentifier, order: Order)

  case class StoreOrderDone(id: OrderIdentifier, order: Order) extends StorageCommand

}

case class OStoredOrder(id: OrderIdentifier, status: OrderStatus, order: Order, bankIdentifier: Option[BankIdentifier]) extends ReplicatedData {

  type T = OStoredOrder

  override def merge(that: OStoredOrder): OStoredOrder = {
    val bankIdentifier = this.bankIdentifier.orElse(that.bankIdentifier)
    val status = this.status.merge(that.status)
    OStoredOrder(this.id, status, this.order, bankIdentifier)
  }
}

case class OrderStatus(name: String)(_predecessors: => Set[OrderStatus], _successors: => Set[OrderStatus]) extends ReplicatedData {

  import OrderStatus._

  type T = OrderStatus

  lazy val predecessors = _predecessors
  lazy val successors = _successors

  override def merge(that: OrderStatus): OrderStatus = {
    val ValidationConflict = Set(Validated, Rejected)
    val ExecutionConflict = Set(Executed, Failed)
    val RejectedExecutedConflict = Set(Rejected, Executed)
    val RejectedFailedConflict = Set(Rejected, Failed)

    Set(this, that) match {
      case ValidationConflict => Validated
      case ExecutionConflict => Executed
      case RejectedExecutedConflict => Executed
      case RejectedFailedConflict => Failed
      case _ => mergeStatus(this, that)
    }

  }

  // source: https://github.com/ReactiveDesignPatterns/CodeSamples/blob/master/chapter13/src/main/scala/com/reactivedesignpatterns/chapter13/MultiMasterCRDT.scala#L31
  def mergeStatus(left: OrderStatus, right: OrderStatus): OrderStatus = {
    /*
     * Keep the left Status in hand and determine whether it is a predecessor of
     * the candidate, moving on to the candidate’s successor if not successful.
     * The list of exclusions is used to avoid performing already determined
     * unsuccessful comparisons again.
     */
    def innerLoop(candidate: OrderStatus, exclude: Set[OrderStatus]): OrderStatus =
      if (isSuccessor(candidate, left, exclude)) {
        candidate
      } else {
        val nextExclude = exclude + candidate
        val branches = candidate.successors.map(succ => innerLoop(succ, nextExclude))
        branches.reduce((l, r) => if (isSuccessor(l, r, nextExclude)) r else l)
      }

    def isSuccessor(candidate: OrderStatus, fixed: OrderStatus, exclude: Set[OrderStatus]): Boolean =
      if (candidate == fixed) true
      else {
        val toSearch = candidate.predecessors -- exclude
        toSearch.exists(pred => isSuccessor(pred, fixed, exclude))
      }

    innerLoop(right, Set.empty)
  }
}

/**
  * New ---> Validated ---> Executed -------|
  *     |              |--> Failed ------ Done
  *     |--> Rejected ----------------------|
  */
object OrderStatus {
  val New: OrderStatus = OrderStatus("new")(Set.empty, Set(Validated, Rejected))
  val Validated: OrderStatus = OrderStatus("validated")(Set(New), Set(Executed, Failed))
  val Rejected: OrderStatus = OrderStatus("rejected")(Set(New), Set(Done))
  val Executed: OrderStatus = OrderStatus("executed")(Set(Validated), Set(Done))
  val Failed: OrderStatus = OrderStatus("failed")(Set(Validated), Set(Done))
  val Done: OrderStatus = OrderStatus("done")(Set(Executed, Failed, Rejected), Set.empty)
}
