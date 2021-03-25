package org.apache.openwhisk.connector.priority

import akka.actor.ActorSystem
import java.util.concurrent.BlockingQueue
import scala.collection.mutable.Map
import org.apache.openwhisk.common.Logging
import scala.concurrent.Future
import java.util.concurrent.PriorityBlockingQueue
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.openwhisk.core.connector.Message
import java.nio.charset.StandardCharsets
import org.apache.openwhisk.common.Counter
import org.apache.openwhisk.core.entity.ActionPriority
import org.apache.openwhisk.core.connector.MessageProducer
import scala.concurrent.ExecutionContext

class PriorityProducer(queues: Map[String, BlockingQueue[(ActionPriority, Array[Byte])]])(implicit
  logging: Logging,
  actorSystem: ActorSystem)
    extends MessageProducer {

  implicit val ec: ExecutionContext = actorSystem.dispatcher
  private val sentCounter = new Counter()

  override def sentCount(): Long = sentCounter.cur

  /** Sends msg to topic. This is an asynchronous operation. */
  override def send(topic: String, msg: Message, retry: Int = 3): Future[RecordMetadata] = {
    implicit val transid = msg.transid

    val queue = queues.getOrElseUpdate(topic, new PriorityBlockingQueue[(ActionPriority, Array[Byte])]())

    Future {
      queue.put((ActionPriority.Normal, msg.serialize.getBytes(StandardCharsets.UTF_8)))
      sentCounter.next()
      new RecordMetadata(new TopicPartition(topic, 0), -1, -1, System.currentTimeMillis(), null, -1, -1)
    }
  }

  /** Closes producer. */
  override def close(): Unit = {
    logging.info(this, "closing priority  producer")
  }
}
