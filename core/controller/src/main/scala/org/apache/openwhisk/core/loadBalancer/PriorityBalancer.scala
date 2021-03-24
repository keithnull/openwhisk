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

class PriorityBalancer(config: WhiskConfig, feedFactory: FeedFactory, controllerInstance: ControllerInstanceId)(implicit
  actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends LeanBalancer(config, feedFactory, controllerInstance) {

  // add priority related code here
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(implicit
    transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val priorityStr = action.annotations.getAs[String](Annotations.ActionPriority).getOrElse("")
    val priority = ActionPriority.fromString(priorityStr)
    println(s"priority = $priority, action = ${msg.activationId}")
    val activationResult = setupActivation(msg, action, invokerName)
    sendActivationToInvoker(messageProducer, msg, invokerName).map(_ => activationResult)
  }
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
