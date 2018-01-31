package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, Props}
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}
import io.bernhardt.reactivepayment.Validator.{OrderRejected, OrderValidated, ValidateOrder}

class Validator extends Actor with ActorLogging {

  def receive = {
    case ValidateOrder(id, order) =>
      if (order.amount < 0) {
        sender() ! OrderRejected(id, order)
      } else {
        // compute fake bank identifier from merchant account - this usually would be done via merchant configuration
        val bankIdentifier = order.account match {
          case PaymentProcessor.MerchantAccountA => PaymentProcessor.BankA
          case PaymentProcessor.MerchantAccountB => PaymentProcessor.BankB
          case PaymentProcessor.MerchantAccountC => PaymentProcessor.BankC
          case _ => PaymentProcessor.BankA
        }
        sender() ! OrderValidated(id, order, bankIdentifier)
      }
  }
}

object Validator {
  def props() = Props(new Validator)

  case class ValidateOrder(id: OrderIdentifier, order: Order)
  case class OrderValidated(id: OrderIdentifier, order: Order, bankIdentifier: BankIdentifier)
  case class OrderRejected(id: OrderIdentifier, order: Order)
}
