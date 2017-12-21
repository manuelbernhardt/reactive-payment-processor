package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.cluster.ddata._
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}

import scala.concurrent.duration._

class OrderStorage extends Actor with ActorLogging {

  import OrderStorage._

  val replicator = DistributedData(context.system).replicator
  implicit val cluster = Cluster(context.system)


  def receive = {
    case request @ RegisterOrder(id, order, _) =>
      val storedOrder = StoredOrder(id, OrderStatus.New, order, None)
      replicator ! Replicator.Update(Key, ORMap.empty[String, StoredOrder], Replicator.WriteLocal, Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }
    case Replicator.UpdateSuccess(Key, Some(request: RegisterOrder)) =>
      request.replyTo ! OrderRegistered(request.id, request.order)
      // TODO handle update failure

    case request @ StoreOrderValidation(id, order, bankIdentifier, _) =>
      val storedOrder = StoredOrder(id, OrderStatus.Validated, order, Some(bankIdentifier))
      replicator ! Replicator.Update(Key, ORMap.empty[String, StoredOrder], Replicator.WriteMajority(5.seconds), Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }
    case Replicator.UpdateSuccess(Key, Some(request: StoreOrderValidation)) =>
      request.replyTo ! OrderValidationStored(request.id, request.order)
      // TODO handle update failure

    case request @ StoreOrderRejection(id, order) =>
      val storedOrder = StoredOrder(id, OrderStatus.Rejected, order, None)
      replicator ! Replicator.Update(Key, ORMap.empty[String, StoredOrder], Replicator.WriteMajority(5.seconds), Some(request)) { orders =>
        orders + (id.i.toString -> storedOrder)
      }

  }

}

object OrderStorage {

  def props() = Props(new OrderStorage())

  val Key = ORMapKey.create[String, StoredOrder]("orders")

  case class RegisterOrder(id: OrderIdentifier, order: Order, replyTo: ActorRef)
  case class OrderRegistered(id: OrderIdentifier, order: Order)

  case class StoreOrderValidation(id: OrderIdentifier, order: Order, bankIdentifier: BankIdentifier, replyTo: ActorRef)
  case class OrderValidationStored(id: OrderIdentifier, order: Order)

  case class StoreOrderRejection(id: OrderIdentifier, order: Order)

}

case class StoredOrder(id: OrderIdentifier, status: OrderStatus, order: Order, bankIdentifier: Option[BankIdentifier]) extends ReplicatedData {

  type T = StoredOrder

  override def merge(that: StoredOrder): StoredOrder = {
    val bankIdentifier = if (this.bankIdentifier.isDefined) this.bankIdentifier else that.bankIdentifier
    val status = this.status.merge(that.status)
    StoredOrder(this.id, status, this.order, bankIdentifier)
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

    Set(this, that) match {
      case ValidationConflict => Validated
      case ExecutionConflict => Executed
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


object OrderStatus {
  val New: OrderStatus = OrderStatus("new")(Set.empty, Set(Validated, Rejected))
  val Validated: OrderStatus = OrderStatus("validated")(Set(New), Set(Executed, Failed))
  val Rejected: OrderStatus = OrderStatus("rejected")(Set(New), Set(Done))
  val Executed: OrderStatus = OrderStatus("executed")(Set(Validated), Set(Done))
  val Failed: OrderStatus = OrderStatus("failed")(Set(Validated), Set(Done))
  val Done: OrderStatus = OrderStatus("done")(Set(Executed, Failed, Rejected), Set.empty)
}
