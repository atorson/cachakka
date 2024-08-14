package com.cachakka.streaming.akka.shard.cache

import java.util.{Date, Objects}
import java.util.concurrent.{ConcurrentLinkedQueue}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import com.cachakka.streaming.akka._

import com.cachakka.streaming.akka.shard.cache.CacheShardRegistry._
import com.cachakka.streaming.akka.shard.cdc.CDCShardExtension.{AnyC, CDC}
import com.cachakka.streaming.akka.shard.cdc._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable._
import scala.reflect.ClassTag
import scala.util.{Success, Try}

object CacheConsumer {
  /**
    * Akka factory methods to define properties of CacheConsumer actor so that it can be created by Akka
    * @param group group ID
    * @param parallelism number of consumers in the group
    * @param index index of this consumer in the group
    * @param rateMultiplier multiplier applied to the base demand rate
    * @param shardOffsets last persisted consumer offsets assigned to this consumer index
    * @param sink downstream sink function
    * @param agentIncludeFilter optional include filter
    * @param agentExcludeFilter optional exclude filter
    * @param ct
    * @tparam E
    * @return
    */
   def props[E<:CDCEntity](group: String, parallelism: Int, index: Int, rateMultiplier: Double = 1.0, shardOffsets: scala.collection.immutable.Map[String,Long], sink: scala.collection.Seq[E] => Unit, agentIncludeFilter: Option[scala.collection.Set[CDCAgent]] = None, agentExcludeFilter: Option[scala.collection.Set[CDCAgent]] = None)(implicit ct: ClassTag[E]) =
     Props.apply(new CacheConsumer[E](group, parallelism, index, rateMultiplier, shardOffsets, sink, agentIncludeFilter, agentExcludeFilter)).withDispatcher("dispatchers.affinity")


  def propsJava[E<:CDCEntity](clazz: Class[E], group: String, parallelism: Int, index: Int, rateMultplier: Double, shardOffsets: java.util.Map[String,Long], sink: java.util.function.Consumer[java.util.List[E]]) = {
    import scala.collection.JavaConverters._
    implicit val ct = ClassTag[E](clazz)
    props[E](group, parallelism, index, rateMultplier, shardOffsets.asScala.toMap, {c: scala.collection.Seq[E] => sink.accept(c.asJava)})
  }


  def propsJava[E<:CDCEntity](clazz: Class[E], group: String, parallelism: Int, index: Int, rateMultplier: Double, shardOffsets: java.util.Map[String,Long], sink: java.util.function.Consumer[java.util.List[E]], agentIncludeFilter: java.util.Set[CDCAgent], agentExcludeFilter: java.util.Set[CDCAgent]) = {
    import scala.collection.JavaConverters._
    implicit val ct = ClassTag[E](clazz)
    props[E](group, parallelism, index, rateMultplier, shardOffsets.asScala.toMap, {c: scala.collection.Seq[E] => sink.accept(c.asJava)},
      Option(agentIncludeFilter).map(_.asScala.toSet), Option(agentExcludeFilter).map(_.asScala.toSet))
  }

}

private case object CacheConsumerPoll

private case class CacheConsumerPollResults[E<:CDCEntity](val entities: scala.collection.Seq[E])

/**
  * Cache consumer actor
  * @param group
  * @param parallelism
  * @param index
  * @param rateMultiplier
  * @param shardOffsets
  * @param sink
  * @param agentIncludeFilter
  * @param agentExcludeFilter
  * @param ct
  * @tparam E
  */
class CacheConsumer[E<:CDCEntity](val group: String, val parallelism: Int, val index:Int, val rateMultiplier: Double, val shardOffsets: scala.collection.immutable.Map[String,Long], val sink: scala.collection.Seq[E] => Unit, val agentIncludeFilter: Option[scala.collection.Set[CDCAgent]], val agentExcludeFilter: Option[scala.collection.Set[CDCAgent]])(implicit val ct: ClassTag[E]) extends Actor with ActorLogging {

  type ShardMeta = (Date, Int)

  type PollResult = (String, ShardMeta, scala.collection.Seq[E])

  import scala.concurrent.duration._

  implicit lazy val ec = AkkaCluster.miscEc

  implicit val cmp: AnyC = findCompanion[AnyC](ct.runtimeClass).get

  val config = CacheShardExtension.config(ct.runtimeClass.getName)

  lazy val timeout = 10.seconds

  /**
    * Shards meta-data (offsets and current backlog sizes)
    */
  val shardsMetaMap = TrieMap[String, ShardMeta]()

  /**
    * Circular buffer used to poll shards
    */
  val shardsSequence = Buffer[CacheShardRegistrationTrait]()

  var startTime = System.currentTimeMillis()

  /**
    * Marker in the circular buffer
    */
  var pollMarker = 0

  val pollingCycle = config.pollingPeriod

  val demandPerConsumer = (config.demandRate * rateMultiplier * pollingCycle)/parallelism

  val maxShardDemand = (config.demandRate * rateMultiplier) /config.shardsRate

  val lastDrainTime = new AtomicLong(startTime)

  /**
    * buffer used to safely pass data to drain thread
    */
  val pollBuffer = new ConcurrentLinkedQueue[PollResult]

  /**
    * Indicator of the downstream backpressure
    */
  val pollBufferSize = new AtomicInteger(0)

  /**
    * Drain thread used to safely invoke downstream function without blocking the actor
    */
  val drainThread = new AtomicReference[Thread]()

  private class DrainThread extends Thread {
    override def run(): Unit = while (Objects.equals(this, drainThread.get)) {
      val r = pollBuffer.poll()
      if (r != null) {
        pollBufferSize.addAndGet(-1*r._3.size)
        sink(r._3)
        shardsMetaMap.put(r._1, r._2)
      }
      val now = System.currentTimeMillis()
      if ((now - lastDrainTime.get) > 1000) lastDrainTime.set(now)
    }
  }

  protected def resetDrainThread = {
    val t = new DrainThread()
    drainThread.set(t)
    t.start()
  }

  /**
    * Poll logic: caps quantity per shard by rate limit and polls enough shards to satisfy total demand constraint
    * @param self
    */
  private def poll(implicit self: ActorRef) = {
    val s = shardsSequence.splitAt(pollMarker)
    val it = (s._2 ++ s._1)
    val counter = new AtomicInteger(0)
    val m = Buffer[(Int,CacheShardRegistrationTrait)]()
    it.find(s => {
      val q = Math.min(maxShardDemand, shardsMetaMap.get(s.shardId).map(_._2).getOrElse(0)).toInt
      val d: Int = counter.addAndGet(q)
      if (d > demandPerConsumer) {
        log.debug(s"Cache consumer ${group->index} has identified $d entities to poll")
        true
      } else {
        m += (q -> s)
        false
      }
    }).foreach(s => pollMarker = shardsSequence.indexOf(s))
    log.debug(s"Cache consumer ${group->index} with address ${self.path} has poll marker set at ${pollMarker}")
    m.foreach(s => {
        log.debug(s"Cache consumer ${group->index} is polling shard ${s._2.shardId} for ${s._1} entities")
        val m = CacheGroupPoll.header(s._2.shardId)(GROUP_ID_KEY -> group, QUANTITY_KEY -> s._1.toString)
        messageShard(m, s._2.replica)
    })
  }

  private def encodeSet(set: scala.collection.Set[String]): String = {
    val s = set.toList.foldLeft("")((x,v) => x + s"$v,")
    if (!s.isEmpty) s.substring(0, s.size-1) else s
  }

  /**
    * Starts actor: trigger registration and polling
    */
  override def preStart(): Unit = {
    super.preStart()
    implicit val self = context.self
    resetDrainThread
    val system = context.system
    system.scheduler.scheduleOnce(1.seconds)({
      val offsets = shardOffsets.flatMap(e => Range(0, CacheShardExtension.getReplicationFactor[E]).map(i => (e._1, i, e._2))).toSeq
      offsets.foreach(e => {
        val m = CacheGroupInitialize.header(e._1)(GROUP_ID_KEY -> group,
          TIMESTAMP_KEY -> e._3.toString,
          INCLUDE_FILTER_KEY -> agentIncludeFilter.map(s => encodeSet(s.map(_.name))).getOrElse(null),
          EXCLUDE_FILTER_KEY -> agentExcludeFilter.map(s => encodeSet(s.map(_.name))).getOrElse(null))
        messageShard(m, e._2)})
      CacheShardExtension.get(system).messageShardRegistry(cacheConsumerRegistration[E](
            CACHE_STATUS_REGISTERING, self, group, parallelism, index))
    })
    system.scheduler.scheduleOnce(pollingCycle.seconds)(self ! CacheConsumerPoll)
    log.info(s"Started cache consumer ${group->index}")
  }

  override def postStop(): Unit = {
    implicit val self = context.self
    drainThread.set(null)
    CacheShardExtension.get(context.system).messageShardRegistry(cacheConsumerRegistration[E](
      CACHE_STATUS_UNREGISTERING, self, group, parallelism, index))
    shardsSequence.foreach(s => {
      val m = CacheGroupStop.header(s.shardId)(GROUP_ID_KEY -> group)
      messageShard(m, s.replica)
    })
    super.postStop()
    log.info(s"Stopped cache consumer ${group->index}")
  }

  private def messageShard(m: ShardedEntity, replica: Int)(implicit self: ActorRef): Unit =  replica match {
    case 0 => AkkaCluster.messageShardingService(m)
    case r => CacheShardExtension.get(AkkaCluster.actorSystem).messageReplicaService(m, r-1)
  }


  private def extractPollResult[T<:ShardedEntity](e: T): Try[PollResult] = Try{
    val children = e.companion.asInstanceOf[CDCEntityResolver[T]].resolveCDCEntities(e)
    val c = children.splitAt(1)
    val args = CacheGroupPoll.decodeKey(c._1.head.getKey)
    val timestamp =  new Date(args.getOrElse(TIMESTAMP_KEY, System.currentTimeMillis().toString).toLong)
    (e.getShardID, (timestamp, args(QUANTITY_KEY).toInt), c._2.map(_.asInstanceOf[E]))
  }


  override def receive: Receive = {
    case x: CacheShardRegistrationTrait  => x.getMessageType match {
      case CACHE_STATUS_REGISTERING => {
        log.debug(s"Cache consumer ${group -> index} has been assigned to poll shard $x")
        val duplicates = shardsSequence.filter(_.shardId == x.shardId)
        shardsSequence --= duplicates
        val optMeta = shardsMetaMap.remove(x.shardId)
        shardsSequence += x
        val m = CacheGroupStart.header(x.shardId)(GROUP_ID_KEY -> group,
          TIMESTAMP_KEY -> optMeta.map(_._1.getTime).getOrElse(startTime).toString,
          INCLUDE_FILTER_KEY -> agentIncludeFilter.map(s => encodeSet(s.map(_.name))).getOrElse(null),
          EXCLUDE_FILTER_KEY -> agentExcludeFilter.map(s => encodeSet(s.map(_.name))).getOrElse(null))
        messageShard(m, x.replica)
      }

      case _ => {
        log.debug(s"Cache consumer ${group -> index} has been unassigned from polling shard $x")
        val duplicates = shardsSequence.filter(_.shardId == x.shardId)
        shardsSequence --= duplicates
        shardsMetaMap.remove(x.shardId)
        val m = CacheGroupStop.header(x.shardId)(GROUP_ID_KEY -> group)
        messageShard(m, x.replica)
        sender ! x.flipStatus(CACHE_STATUS_UNSUBSCRIBED)
      }
    }

    case  CacheConsumerPoll => {
      if ((System.currentTimeMillis() - lastDrainTime.get()) > 10*1000*pollingCycle){
        log.error(s"Cache consumer ${group->index} drain thread has frozen, restarting it")
        resetDrainThread
      }
      // backpressure: skip next poll-cycle
      if (pollBufferSize.get() <= 2* demandPerConsumer) {
        poll(context.self)
      } else {
        log.debug(s"Cache consumer ${group->index} downstream is back-pressured: skipping a polling cycle...")
      }
      val s = context.self
      context.system.scheduler.scheduleOnce(pollingCycle.seconds)(s ! CacheConsumerPoll)
    }

    case m: ShardedEntity if (m.getOperation.isInstanceOf[CacheGroupPoll]) => extractPollResult(m) match {
       case Success(r) => {
         log.debug(s"Polled ${r._3.size} entities from the shard ${r._1} with ${r._2._2} entities remaining backlogged")
         if (!shardsMetaMap.contains(r._1)) {
           val duplicates = shardsSequence.filter(_.shardId == r._1)
           shardsSequence --= duplicates
           duplicates.foreach(d => {
             val result = d.flipStatus(CACHE_STATUS_SUBSCRIBED)
             shardsSequence += result
             CacheShardExtension.get(context.system).messageShardRegistry(result)
           })
         }
         //pass data in bulk to the drain thread
         if (r._3.isEmpty) shardsMetaMap.put(r._1, r._2) else {
           pollBuffer.add(r)
           pollBufferSize.addAndGet(r._3.size)
         }
       }
       case _ => {log.error(s"Failed to extract cache poll results from $m")}
     }

    case x => log.warning(s"Cache consumer ${group->index} has received unexpected message $x")

  }

}
