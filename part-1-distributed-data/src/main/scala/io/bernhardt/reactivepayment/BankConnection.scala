package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, Props}
import io.bernhardt.reactivepayment.BankConnection.{ExecuteOrder, OrderExecutionSucceeded}
import io.bernhardt.reactivepayment.PaymentProcessor.{BankIdentifier, Order, OrderIdentifier}

/**
  * Simulates a secure connection with a bank
  */
class BankConnection(bankIdentifier: BankIdentifier) extends Actor with ActorLogging {

  log.info("Starting secured bank connection for bank {}", bankIdentifier)

  def receive: Receive = {
    case ExecuteOrder(id, order) =>
      // appear to take time
      Thread.sleep(150)
      sender() ! OrderExecutionSucceeded(id, order)
  }


}

object BankConnection {

  def props(identifier: BankIdentifier) = Props(new BankConnection(identifier))

  case class ExecuteOrder(id: OrderIdentifier, order: Order)
  case class OrderExecutionSucceeded(id: OrderIdentifier, order: Order)
  case class OrderExecutionFailed(id: OrderIdentifier, order: Order)

}
