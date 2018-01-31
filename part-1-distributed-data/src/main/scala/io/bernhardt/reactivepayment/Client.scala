package io.bernhardt.reactivepayment

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.bernhardt.reactivepayment.Client.ProcessOrder
import io.bernhardt.reactivepayment.PaymentProcessor.Order

class Client(orderStorage: ActorRef, validator: ActorRef) extends Actor with ActorLogging {

  def receive: Receive = {
    case ProcessOrder(order) =>
      context.actorOf(OrderHandler.props(order, sender(), orderStorage, validator))

  }
}

object Client {

  def props(orderStorage: ActorRef, validator: ActorRef) = Props(new Client(orderStorage, validator))

  case class ProcessOrder(order: Order)

}
