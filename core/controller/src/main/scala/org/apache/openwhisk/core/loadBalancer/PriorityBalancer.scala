package org.apache.openwhisk.core.loadBalancer

import spray.json.DefaultJsonProtocol._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import akka.stream.ActorMaterializer
import org.apache.openwhisk.core.WhiskConfig
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import scala.concurrent.Future
import org.apache.openwhisk.core.connector.ActivationMessage
// import org.apache.kafka.clients.producer.RecordMetadata
// import scala.util.{Failure, Success}
// import akka.event.Logging.InfoLevel
import java.util.concurrent.PriorityBlockingQueue
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import java.util.Comparator
import org.apache.openwhisk.core.entity.ActionPriority.High
import org.apache.openwhisk.core.entity.ActionPriority.Low
import org.apache.openwhisk.core.entity.ActionPriority.Normal

class PriorityBalancer(config: WhiskConfig, feedFactory: FeedFactory, controllerInstance: ControllerInstanceId)(implicit
  actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends LeanBalancer(config, feedFactory, controllerInstance) {

  protected val priorityQueue =
    new PriorityBlockingQueue[(ActionPriority, ActivationMessage)](10, new PriorityMessageComparator)

  // add priority related code here
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(implicit
    transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val priorityStr = action.annotations.getAs[String](Annotations.ActionPriority).getOrElse("")
    val priority = ActionPriority.fromString(priorityStr)
    logging.info(this, s"action = ${action.name}, priority = $priority, activation = ${msg.activationId}")
    val activationResult = setupActivation(msg, action, invokerName)
    priorityQueue.put((priority, msg))
    Future(activationResult)
  }

  // create an applier that constantly applies messages in the priority queue
  private def makeALocalThreadedApplier(duration: FiniteDuration): Unit = {
    val thread = new Thread {
      override def run {

        def runOnce(): Unit = {
          Option(priorityQueue.poll(duration.toMillis, TimeUnit.MILLISECONDS)) match {
            case Some((p, msg)) => {
              logging.info(this, s"Execute activation: priority = $p, activation = ${msg.activationId}")
              sendActivationToInvoker(messageProducer, msg, invokerName)
            }
            case None => Unit
          }
          runOnce()
        }

        runOnce()
      }
    }
    thread.start
  }
  makeALocalThreadedApplier(FiniteDuration(50, TimeUnit.MILLISECONDS))
}

object PriorityBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit
    actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {

    new PriorityBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)
  }

  def requiredProperties =
    ExecManifest.requiredProperties ++
      wskApiHost
}

class PriorityMessageComparator extends Serializable with Comparator[(ActionPriority, ActivationMessage)] {

  def compare(x1: (ActionPriority, ActivationMessage), x2: (ActionPriority, ActivationMessage)): Int = {
    val p1 = x1._1
    val p2 = x2._1
    p1 match {
      case High =>
        p2 match {
          case High => 0
          case _    => 1
        }
      case Low =>
        p2 match {
          case Low => 0
          case _   => -1
        }
      case Normal =>
        p2 match {
          case High   => -1
          case Normal => 0
          case Low    => 1
        }
    }
  }
}
