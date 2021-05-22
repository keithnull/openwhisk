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
import java.time

import org.apache.openwhisk.core.entity.ActionPriority
import scala.collection.mutable.Queue

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

  val maxAgePerLevel = 30000
  // (priority, topic, enqueueTime ,partition, offset, bytes, age)
  type QueueItem = (ActionPriority, String, time.LocalTime, Int, Long, Array[Byte], Int)

  /** Indicates the fill operation has completed. */
  private case class FillCompleted(messages: Seq[QueueItem])
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

  require(consumersWithPriority.size >= 1, "there should be at least one consumer provided")

  // double-buffer to make up for message bus read overhead
  val maxPipelineDepth = maximumHandlerCapacity * 2

  require(
    consumersWithPriority(0)._2.maxPeek <= maxPipelineDepth,
    "consumer may not yield more messages per peek than permitted by max depth")

  private val pipelineFillThreshold = maxPipelineDepth - consumersWithPriority(0)._2.maxPeek

  private val outstandingMessages = consumersWithPriority.map { case (p, consumer) => { (p, Queue.empty[QueueItem]) } }
  private var handlerCapacity = maximumHandlerCapacity

  private implicit val tid = TransactionId.dispatcher

  // used for each consumer to make sure that the overall polling duration is not exceeded
  private val dividedLongPollDuration = longPollDuration.div(consumersWithPriority.size)

  logging.info(this, s"${consumersWithPriority.size} consumers provided: ${consumersWithPriority.map(_._1)}")
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
      outstandingMessages foreach {
        case (priority, queue) => queue.enqueue(messages.filter(_._1 == priority): _*)
      }
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
    if (shouldFillQueue()) {
      Future {
        blocking {
          val records = consumersWithPriority.foldLeft(Seq.empty[QueueItem])((acc, cp) => {
            val (priority, consumer) = cp
            if (!shouldFillSingleQueue(priority)) {
              acc
            } else {
              acc ++ consumer.peek(longPollDuration).map {
                // just prepend its priority here
                case (topic, partition, offset, bytes) =>
                  (priority, topic, time.LocalTime.now(), partition, offset, bytes, 0)
              }
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

  // update items in the queue with mechanism like aging
  private def updateQueue(): Unit = {
    var starvingItems = Seq.empty[QueueItem]
    outstandingMessages.reverse foreach {
      case (priority, queue) => {
        val oldSize = queue.size
        val newItems = queue.map {
          case (p, topic, t, partition, offset, bytes, age) => (p, topic, t, partition, offset, bytes, age + 1)
        } ++ starvingItems
        queue.clear()
        queue.enqueue(newItems: _*)
        if (logHandoff && starvingItems.size > 0) {
          logging.debug(
            this,
            s"${starvingItems.size} items added to $priority due to aging ($oldSize -> ${queue.size})")
        }
        if (priority == ActionPriority.High) {
          starvingItems = Seq.empty[QueueItem]
        } else {
          val oldSize = queue.size
          // dequeue all items that have been waiting too long
          starvingItems = queue.dequeueAll(_._7 > maxAgePerLevel).map {
            case (p, topic, t, partition, offset, bytes, age) => (p, topic, t, partition, offset, bytes, 0)
          }
          if (logHandoff && starvingItems.size > 0) {
            logging.debug(
              this,
              s"${starvingItems.size} items removed from $priority due to aging ($oldSize -> ${queue.size})")
          }
        }
      }
    }
  }

  /** Send as many messages as possible to the handler. */
  @tailrec
  private def sendOutstandingMessages(): Unit = {
    updateQueue()
    if (handlerCapacity > 0) {

      val targetQueue = outstandingMessages.find(_._2.size > 0)
      targetQueue match {
        case Some((p, q)) => {
          val occupancy = outstandingMessages.map {
            case (p, q) => (p, q.size)
          }
          val (priority, topic, t, partition, offset, bytes, age) = q.head
          q.dequeue()

          if (logHandoff)
            logging.debug(this, s"[Priority: $p] processing $topic[$partition][$offset] ($occupancy/$handlerCapacity)")
          handler(bytes)
          handlerCapacity -= 1

          sendOutstandingMessages()
        }
        case None => {
          // no queue available
        }
      }
    }
  }

  private def shouldFillSingleQueue(priority: ActionPriority): Boolean = {
    return !outstandingMessages.find {
      case (p, q) => p == priority && q.size <= pipelineFillThreshold
    }.isEmpty
  }

  private def shouldFillQueue(): Boolean = {
    val occupancy = outstandingMessages.map {
      case (p, q) => (p, q.size)
    }
    // only if there's some queue that has no more items than threshold
    // AND the total number of items in all queues is no more than threshold times the number of queues
    if (occupancy
          .filter(_._2 <= pipelineFillThreshold)
          .size > 0 && occupancy.map(_._2).sum <= pipelineFillThreshold * occupancy.size) {
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
