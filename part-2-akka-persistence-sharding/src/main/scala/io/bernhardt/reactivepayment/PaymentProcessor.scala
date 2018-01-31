package io.bernhardt.reactivepayment

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorSystem, Props, SupervisorStrategy}
import akka.pattern.ask
import akka.util.Timeout
import io.bernhardt.reactivepayment.Client.ProcessOrder
import io.bernhardt.reactivepayment.PaymentProcessor._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * Simple payment processor for imaginary payment orders
  */
trait PaymentProcessor {

  def processPayment(order: Order): Future[OrderResult]

  def processPaymentSync(order: Order): OrderResult

}

case class ReactivePaymentProcessor(system: ActorSystem)(implicit ec: ExecutionContext) extends PaymentProcessor {

  class ActorPaymentProcessor extends Actor with ActorLogging {

    val validator = context.actorOf(Validator.props())
    val executor = context.actorOf(Executor.props())
    val merchant = context.actorOf(Merchant.props(validator, executor))
    val client = context.actorOf(Client.props(merchant))

    def receive = {
      case order: ProcessOrder =>
        client.forward(order)
      case msg => unhandled(msg)
    }
  }

  object ActorPaymentProcessor {
    def props() = Props(new ActorPaymentProcessor)
  }

  val processor = system.actorOf(ActorPaymentProcessor.props(), "processor")


  override def processPayment(order: Order): Future[OrderResult] = {
    implicit val timeout: Timeout = Timeout(10.seconds)
    (processor ? Client.ProcessOrder(order)).map {
      case Merchant.OrderRejected(id) =>
        OrderFailed(Some(id))
      case Merchant.OrderExecuted(id) =>
        OrderSucceeded(id)
      case Merchant.OrderFailed(id) =>
        OrderFailed(Some(id))
    } recover {
      case NonFatal(_) =>
        OrderFailed(None)
    }
  }

  override def processPaymentSync(order: Order): OrderResult =
    Await.result[OrderResult](processPayment(order), 10.seconds)
}


object PaymentProcessor {

  case class Order(account: MerchantAccount, creditCardToken: CreditCardToken, amount: BigDecimal, currency: Currency, usage: String)

  sealed trait OrderResult
  case class OrderSucceeded(id: OrderIdentifier) extends OrderResult
  case class OrderFailed(id: Option[OrderIdentifier]) extends OrderResult

  case class MerchantAccount(a: String) extends AnyVal
  val MerchantAccountA = MerchantAccount("A")
  val MerchantAccountB = MerchantAccount("B")
  val MerchantAccountC = MerchantAccount("C")

  case class CreditCardToken(t: String) extends AnyVal

  case class BankIdentifier(b: String) extends AnyVal
  val BankA = BankIdentifier("A")
  val BankB = BankIdentifier("B")
  val BankC = BankIdentifier("C")

  case class OrderIdentifier(i: UUID) extends AnyVal
  object OrderIdentifier {
    def generate = OrderIdentifier(UUID.randomUUID())
  }

  case class Currency(name: String) extends AnyVal
  val EUR = Currency("EUR")

}

