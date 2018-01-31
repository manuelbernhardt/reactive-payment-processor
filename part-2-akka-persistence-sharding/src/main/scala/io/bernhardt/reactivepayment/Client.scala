package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.bernhardt.reactivepayment.Client.ProcessOrder
import io.bernhardt.reactivepayment.PaymentProcessor.Order

class Client(merchant: ActorRef) extends Actor with ActorLogging {

  def receive: Receive = {
    case ProcessOrder(order) =>
      merchant forward Merchant.ProcessNewOrder(order)
  }
}

object Client {

  def props(merchant: ActorRef) = Props(new Client(merchant))

  case class ProcessOrder(order: Order)

}
