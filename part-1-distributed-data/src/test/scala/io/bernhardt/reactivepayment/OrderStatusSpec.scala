package io.bernhardt.reactivepayment

import org.scalatest.{MustMatchers, WordSpec}

/**
  * New ---> Validated ---> Executed -------|
  *     |              |--> Failed ------ Done
  *     |--> Rejected ----------------------|
  */
class OrderStatusSpec extends WordSpec with MustMatchers {

  "The OrderStatus CRDT" must {

    "resolve (Validated Rejected) conflicts to Validated" in {
      OrderStatus.Validated.merge(OrderStatus.Rejected) must be (OrderStatus.Validated)
    }

    "resolve (Executed Failed) conflicts to Executed" in {
      OrderStatus.Executed.merge(OrderStatus.Failed) must be (OrderStatus.Executed)
    }

    "resolve (Rejected Executed) conflicts as Executed" in {
      OrderStatus.Rejected.merge(OrderStatus.Executed) must be (OrderStatus.Executed)
    }

    "resolve (Rejected Failed) conflicts as Failed" in {
      OrderStatus.Rejected.merge(OrderStatus.Failed) must be (OrderStatus.Failed)
    }

  }

}
