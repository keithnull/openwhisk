package org.apache.openwhisk.connector.priority

import java.util.concurrent.BlockingQueue
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector.MessageConsumer
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import org.apache.openwhisk.core.entity.ActionPriority

class PriorityComsumer(queue: BlockingQueue[(ActionPriority, Array[Byte])], override val maxPeek: Int)(implicit
  logging: Logging)
    extends MessageConsumer {

  /**
   * Long poll for messages. Method returns once message available but no later than given
   * duration.
   *
   * @param duration the maximum duration for the long poll
   */
  override def peek(duration: FiniteDuration, retry: Int): Iterable[(String, Int, Long, Array[Byte])] = {
    Option(queue.poll(duration.toMillis, TimeUnit.MILLISECONDS))
      .map(record => Iterable(("", 0, 0L, record._2))) // only keep the message and drop priority
      .getOrElse(Iterable.empty)
  }

  /**
   * There's no cursor to advance since that's done in the poll above.
   */
  override def commit(retry: Int): Unit = { /*do nothing*/ }

  override def close(): Unit = {
    logging.info(this, s"closing priority consumer")
  }
}
