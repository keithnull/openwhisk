package org.apache.openwhisk.connector.priority

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.Map
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.Success
import scala.util.Try

import akka.actor.ActorSystem
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.MessageConsumer
import org.apache.openwhisk.core.connector.MessageProducer
import org.apache.openwhisk.core.connector.MessagingProvider
import org.apache.openwhisk.core.entity.ByteSize
import java.util.concurrent.PriorityBlockingQueue
import org.apache.openwhisk.core.entity.ActionPriority

/**
 * A simple implementation of MessagingProvider.
 */
object PriorityMessagingProvider extends MessagingProvider {

  /** Map to hold message queues, the key is the topic */
  val queues: Map[String, BlockingQueue[(ActionPriority, Array[Byte])]] =
    new TrieMap[String, BlockingQueue[(ActionPriority, Array[Byte])]]

  def getConsumer(config: WhiskConfig, groupId: String, topic: String, maxPeek: Int, maxPollInterval: FiniteDuration)(
    implicit
    logging: Logging,
    actorSystem: ActorSystem): MessageConsumer = {

    // use Priority Queue here instead
    val queue = queues.getOrElseUpdate(topic, new PriorityBlockingQueue[(ActionPriority, Array[Byte])]())

    new PriorityComsumer(queue, maxPeek)
  }

  def getProducer(config: WhiskConfig, maxRequestSize: Option[ByteSize] = None)(implicit
    logging: Logging,
    actorSystem: ActorSystem): MessageProducer =
    new PriorityProducer(queues)

  def ensureTopic(config: WhiskConfig, topic: String, topicConfigKey: String, maxMessageBytes: Option[ByteSize] = None)(
    implicit logging: Logging): Try[Unit] = {
    if (queues.contains(topic)) {
      Success(logging.info(this, s"topic $topic already existed"))
    } else {
      queues.put(topic, new LinkedBlockingQueue[(ActionPriority, Array[Byte])]())
      Success(logging.info(this, s"topic $topic created"))
    }
  }
}
