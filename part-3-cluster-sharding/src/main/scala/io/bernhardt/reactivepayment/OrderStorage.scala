package io.bernhardt.reactivepayment

import akka.cluster.ddata._

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
