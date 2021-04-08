package org.apache.openwhisk.core.connector

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.util.Failure
import org.apache.kafka.clients.consumer.CommitFailedException
import akka.actor.FSM
import akka.pattern.pipe
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.common.TransactionId
import scala.collection.mutable.PriorityQueue

import org.apache.openwhisk.core.entity.ActionPriority

// forgive me for copy-and-paste here, will modify in the future

object AggregatedMessageFeed {
  protected sealed trait FeedState
  protected[connector] case object Idle extends FeedState
  protected[connector] case object FillingPipeline extends FeedState
  protected[connector] case object DrainingPipeline extends FeedState

  protected sealed trait FeedData
  private case object NoData extends FeedData

  /** Indicates the consumer is ready to accept messages from the message bus for processing. */
  object Ready

  // (priority, topic, partition, offset, bytes)
  type QueueItem = (ActionPriority, String, Int, Long, Array[Byte])

  /** Indicates the fill operation has completed. */
  private case class FillCompleted(messages: Seq[QueueItem])

  implicit val KeyOrdering = new Ordering[QueueItem] {
    override def compare(x: QueueItem, y: QueueItem): Int = {
      val (px, py) = (x._1, y._1)
      if (px != py) { // priority first
        (px, py) match {
          case (ActionPriority.Low, _) | (ActionPriority.Normal, ActionPriority.High) => -1
          case _                                                                      => 1
        }
      } else { // then offset
        if (x._4 > y._4) 1 else if (x._4 < y._4) -1 else 0
      }
    }
  }
}

class AggregatedMessageFeed(
  description: String,
  logging: Logging,
  consumersWithPriority: Seq[(ActionPriority, MessageConsumer)],
  maximumHandlerCapacity: Int,
  longPollDuration: FiniteDuration,
  handler: Array[Byte] => Future[Unit],
  autoStart: Boolean = true,
  logHandoff: Boolean = true)
    extends FSM[AggregatedMessageFeed.FeedState, AggregatedMessageFeed.FeedData] {
  import AggregatedMessageFeed._

  require(consumersWithPriority.size >= 1, "there should be at least one conumser provided")

  // double-buffer to make up for message bus read overhead
  val maxPipelineDepth = maximumHandlerCapacity * 2

  require(
    consumersWithPriority(0)._2.maxPeek <= maxPipelineDepth,
    "consumer may not yield more messages per peek than permitted by max depth")

  private val pipelineFillThreshold = maxPipelineDepth - consumersWithPriority(0)._2.maxPeek

  private val outstandingMessages = PriorityQueue.empty[QueueItem]
  private var handlerCapacity = maximumHandlerCapacity

  private implicit val tid = TransactionId.dispatcher

  // used for each consumer to make sure that the overall polling duration is not exceeded
  private val dividedLongPollDuration = longPollDuration.div(consumersWithPriority.size)

  logging.info(this, s"${consumersWithPriority.size} consumers privoded: ${consumersWithPriority.map(_._1)}")
  logging.info(
    this,
    s"handler capacity = $maximumHandlerCapacity, pipeline fill at = $pipelineFillThreshold, pipeline depth = $maxPipelineDepth")

  when(Idle) {
    case Event(Ready, _) =>
      fillPipeline()
      goto(FillingPipeline)

    case _ => stay
  }

  // wait for fill to complete, and keep filling if there is
  // capacity otherwise wait to drain
  when(FillingPipeline) {
    case Event(MessageFeed.Processed, _) =>
      updateHandlerCapacity()
      sendOutstandingMessages()
      stay

    case Event(FillCompleted(messages), _) =>
      outstandingMessages.enqueue(messages: _*)
      sendOutstandingMessages()

      if (shouldFillQueue()) {
        fillPipeline()
        stay
      } else {
        goto(DrainingPipeline)
      }

    case _ => stay
  }

  when(DrainingPipeline) {
    case Event(MessageFeed.Processed, _) =>
      updateHandlerCapacity()
      sendOutstandingMessages()
      if (shouldFillQueue()) {
        fillPipeline()
        goto(FillingPipeline)
      } else stay

    case _ => stay
  }

  onTransition { case _ -> Idle => if (autoStart) self ! Ready }
  startWith(Idle, AggregatedMessageFeed.NoData)
  initialize()

  private implicit val ec = context.system.dispatchers.lookup("dispatchers.kafka-dispatcher")

  private def fillPipeline(): Unit = {
    if (outstandingMessages.size <= pipelineFillThreshold) {
      Future {
        blocking {
          val records = consumersWithPriority.foldLeft(Seq.empty[QueueItem])((acc, cp) => {
            if (acc.size > 0) { // stop further collecting records
              acc
            }
            val (priority, consumer) = cp
            acc ++ consumer.peek(longPollDuration).map {
              // just prepend its priority here
              case (topic, partition, offset, bytes) => (priority, topic, partition, offset, bytes)
            }
          })
          FillCompleted(records)
        }
      }.andThen {
          case Failure(e: CommitFailedException) =>
            logging.error(this, s"failed to commit $description consumer offset: $e")
          case Failure(e: Throwable) => logging.error(this, s"exception while pulling new $description records: $e")
        }
        .recover {
          case _ => FillCompleted(Seq.empty)
        }
        .pipeTo(self)
    } else {
      logging.error(this, s"dropping fill request until $description feed is drained")
    }
  }

  /** Send as many messages as possible to the handler. */
  @tailrec
  private def sendOutstandingMessages(): Unit = {
    val occupancy = outstandingMessages.size
    if (occupancy > 0 && handlerCapacity > 0) {
      // Easiest way with an immutable queue to cleanly dequeue
      // Head is the first elemeent of the queue, desugared w/ an assignment pattern
      // Tail is everything but the first element, thus mutating the collection variable
      val (priority, topic, partition, offset, bytes) = outstandingMessages.head
      outstandingMessages.dequeue()

      if (logHandoff)
        logging.debug(
          this,
          s"[Priority: $priority] processing $topic[$partition][$offset] ($occupancy/$handlerCapacity)")
      handler(bytes)
      handlerCapacity -= 1

      sendOutstandingMessages()
    }
  }

  private def shouldFillQueue(): Boolean = {
    val occupancy = outstandingMessages.size
    if (occupancy <= pipelineFillThreshold) {
      logging.debug(
        this,
        s"$description pipeline has capacity: $occupancy <= $pipelineFillThreshold ($handlerCapacity)")
      true
    } else {
      logging.debug(this, s"$description pipeline must drain: $occupancy > $pipelineFillThreshold")
      false
    }
  }

  private def updateHandlerCapacity(): Int = {
    logging.debug(self, s"$description received processed msg, current capacity = $handlerCapacity")

    if (handlerCapacity < maximumHandlerCapacity) {
      handlerCapacity += 1
      handlerCapacity
    } else {
      if (handlerCapacity > maximumHandlerCapacity) logging.error(self, s"$description capacity already at max")
      maximumHandlerCapacity
    }
  }
}
