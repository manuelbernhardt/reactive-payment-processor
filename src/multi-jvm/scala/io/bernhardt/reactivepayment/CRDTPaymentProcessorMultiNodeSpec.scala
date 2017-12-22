package io.bernhardt.reactivepayment

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import com.typesafe.config.ConfigFactory
import io.bernhardt.reactivepayment.PaymentProcessor.{EUR, MerchantAccount, Order, OrderSucceeded}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CRDTPaymentMultiJvmNode1 extends CRDTPaymentProcessorMultiNode

class CRDTPaymentMultiJvmNode2 extends CRDTPaymentProcessorMultiNode

class CRDTPaymentMultiJvmNode3 extends CRDTPaymentProcessorMultiNode

class CRDTPaymentProcessorMultiNode extends MultiNodeSpec(CRDTPaymentMultiNodeConfig) with ScalaTestMultiNodeSpec with ScalaFutures {

  override implicit val patienceConfig = PatienceConfig(scaled(Span(15, Seconds)))

  import CRDTPaymentMultiNodeConfig._

  override def initialParticipants = 3


  "A CRDT Payment Processor" must {

    var processor: Option[PaymentProcessor] = None

    "start all nodes" in within(15.seconds) {
      Cluster(system).subscribe(testActor, classOf[MemberUp])
      expectMsgClass(classOf[CurrentClusterState])
      Cluster(system) join node(node1).address

      runOn(node1) {
        CRDTPaymentProcessor(system)
      }
      runOn(node2) {
        CRDTPaymentProcessor(system)
      }
      runOn(node3) {
       processor = Some(CRDTPaymentProcessor(system))

      }

      receiveN(3).collect { case MemberUp(m) => m.address }.toSet must be(
        Set(node(node1).address, node(node2).address, node(node3).address)
      )

      testConductor.enter("all-up")
    }

    "be able to process a valid order" in within(15.seconds) {
      runOn(node3) {
        val order = Order(PaymentProcessor.MerchantAccountA, BigDecimal(10.00), EUR, "Test node 1")
        processor.get.processPayment(order).futureValue mustBe an[OrderSucceeded]
      }

      enterBarrier("order-processed")
    }
  }
}


object CRDTPaymentMultiNodeConfig extends MultiNodeConfig {
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

  nodeConfig(node1)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[bank-A]
    """.stripMargin))

  nodeConfig(node2)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[bank-B]
    """.stripMargin))

    nodeConfig(node3)(ConfigFactory.parseString(
    """
      |akka.cluster.roles=[bank-C]
    """.stripMargin))


  commonConfig(ConfigFactory.parseString(
    """
      |akka.loglevel=INFO
      |akka.actor.provider = cluster
      |akka.remote.artery.enabled = on
      |akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
      |akka.coordinated-shutdown.terminate-actor-system = off
      |akka.cluster.run-coordinated-shutdown-when-down = off
    """.stripMargin))
}