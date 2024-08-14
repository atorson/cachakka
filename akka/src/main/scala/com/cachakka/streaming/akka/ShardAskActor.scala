package com.cachakka.streaming.akka

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.AskTimeoutException
import com.google.common.base.Strings

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future, Promise, TimeoutException}
import scala.util.{Failure, Try}

/**
  * Any cluster message can be decorated with this trait so that ShardAskManager can be used to provide a high-throughput Ask/req-response pattern
  * @tparam E
  */
trait AsyncAskMessage[E<:ClusterMessage]{

  /**
    * Used to correlate request message with response message
    * @return
    */
  def correlationId: String

  /**
    * Mutation setter
    * @param correlationId
    * @return
    */
  def withCorrelationId(correlationId: String): E

}

/**
  * This class implements the high-throughput version of the Akka 'ask' pattern
  * The main idea is to use a long-living single actor instead of a disposable actor per call
  * Holds a state of correlation IDs for all in-flight messages
  * Periodically executes a coarse timeout check
  * @param coarseTimeout
  * @param coarseTimeoutCheckPeriod
  */
class ShardAskManager(val coarseTimeout: FiniteDuration, val coarseTimeoutCheckPeriod: FiniteDuration){

  private val askMap = new ConcurrentHashMap[String, (Long, Promise[ClusterMessage])]()

  private val initializer = new AskMapInitializer

  lazy val askActor = AkkaCluster.actorSystem.actorOf(Props(new ShardAskActor(coarseTimeout, coarseTimeoutCheckPeriod, askMap, initializer)).withDispatcher("dispatchers.sharding"))

  import scala.concurrent.duration._

  def syncCallShardingService(m: ShardedEntity with AsyncAskMessage[_], timeout: FiniteDuration = 10.seconds): Try[ClusterMessage] = {
    Try{Await.result(asyncCallShardingService(m), timeout)}
  }

  /**
    * Async-IO Ask API
    * @param m
    * @param timeout optional. Don't specify it in high-throuput context as there is a cost for it. Rely on default/coarse timeouting in such cases
    * @return
    */
  def asyncCallShardingService(m: ShardedEntity with AsyncAskMessage[_], timeout: Option[FiniteDuration] = None): Future[ClusterMessage] = {
    val autoShardedMessage = m match {
      case v: AutoShardedEntity[_] => v.processShardId(m.getOperation).asInstanceOf[ShardedEntity with AsyncAskMessage[_]]
      case _ => m
    }
    val validatedMessage = Strings.isNullOrEmpty(autoShardedMessage.correlationId) match {
      case true => {
        autoShardedMessage.withCorrelationId(UUID.randomUUID().toString).asInstanceOf[ShardedEntity with AsyncAskMessage[_]]
      }
      case _ => m
    }
    val p = askMap.computeIfAbsent(validatedMessage.correlationId, initializer)._2
    timeout match {
      case Some(t) => AkkaCluster.actorSystem.scheduler.scheduleOnce(t) {
        p tryComplete Failure(
          new AskTimeoutException(s"Shard ask manager timed out on node ${AkkaCluster.selfAddress.host.getOrElse("")} after [${t.toMillis} ms] with async message call $m"))
      }(AkkaCluster.miscEc)
      case _ => {}
    }
    AkkaCluster.shardingEntityServicesRegionActor.get()(validatedMessage.getOperation.getServiceName).tell(validatedMessage, askActor)
    p.future
  }

}

class AskMapInitializer extends java.util.function.Function[String, (Long, Promise[ClusterMessage])] {
  override def apply(t: String) = (System.currentTimeMillis(), Promise[ClusterMessage]())
}

case object ShardAskActorCoarseTimeoutCheck

class ShardAskActor(val coarseTimeout: FiniteDuration, val coarseTimeoutCheckPeriod: FiniteDuration, askMap: ConcurrentHashMap[String, (Long, Promise[ClusterMessage])], initializer: AskMapInitializer) extends Actor with ActorLogging{

  override def preStart(): Unit = {
    super.preStart()
    val system = context.system
    val self = context.self
    system.scheduler.scheduleOnce(coarseTimeoutCheckPeriod)(self ! ShardAskActorCoarseTimeoutCheck)(AkkaCluster.miscEc)
  }

  override def receive: Receive = {
    case m: ClusterMessage with AsyncAskMessage[_] => Option(askMap.remove(m.correlationId)).foreach(_._2.trySuccess(m))
    case ShardAskActorCoarseTimeoutCheck => {
      import scala.collection.JavaConverters._
      val timeoutMillis = coarseTimeout.toMillis
      val threshold = System.currentTimeMillis() - timeoutMillis
      val failures = askMap.asScala.filter(x => x._2._1 < threshold).toSeq
      failures.foreach(x => {
        x._2._2.tryFailure(new AskTimeoutException(s"Shard ask manager timed out on node ${AkkaCluster.selfAddress.host.getOrElse("")} after [$timeoutMillis ms]"))
        askMap.remove(x._1)
      })
      val system = context.system
      val self = context.self
      system.scheduler.scheduleOnce(coarseTimeoutCheckPeriod)(self ! ShardAskActorCoarseTimeoutCheck)(AkkaCluster.miscEc)
    }
  }
}
