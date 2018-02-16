package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, Props}
import io.bernhardt.reactivepayment.PaymentProcessor.{Order, OrderIdentifier}
import io.bernhardt.reactivepayment.Validator.ValidateOrder

class Validator extends Actor with ActorLogging {

  def receive = {
    case ValidateOrder(id, order) =>
      if (order.amount < 0) {
        sender() ! Merchant.ProcessOrderRejected(id, order)
      } else {
        // compute fake bank identifier from merchant account - this usually would be done via merchant configuration
        val bankIdentifier = order.account match {
          case PaymentProcessor.MerchantAccountA => PaymentProcessor.BankA
          case PaymentProcessor.MerchantAccountB => PaymentProcessor.BankB
          case PaymentProcessor.MerchantAccountC => PaymentProcessor.BankC
          case _ => PaymentProcessor.BankA
        }
        sender() ! Merchant.ProcessOrderValidated(id, order, bankIdentifier)
      }
  }
}

object Validator {
  def props() = Props(new Validator)

  case class ValidateOrder(id: OrderIdentifier, order: Order)
}
