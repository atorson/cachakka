package com.cachakka.streaming.akka.shard.cdc



import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import akka.actor.{ActorRef, ActorSystem, Cancellable, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, PoisonPill}
import akka.cluster.Cluster
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import com.google.common.base.Strings
import com.google.common.reflect.TypeToken
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.akka._
import com.cachakka.streaming.akka.shard.cdc.CDCShardExtension.CDC

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scalapb.descriptors._

/**
  * Akka-CDC extension (is loaded together with the Akka actor system)
  * @param actorSystem
  */
class CDCShardExtensionImpl(actorSystem: ActorSystem) extends Extension{

  /**
    * Cass context (acts like Quill-IO DB client)
    */
  lazy val cassContext: CDCCassandraContext = {
    AkkaCluster.injector.getInstance(classOf[CDCCassandraContext])
  }

  actorSystem.registerOnTermination(cassContext.close())

  val cdcHttpFacadeSingletonActor: AtomicReference[ActorRef] = {
    val r =  new AtomicReference[ActorRef]()
    AkkaCluster.onMemberUpCallbacks += 30 -> (() => r.set(innerStartCdcHttpSingleton))
    r
  }

  protected def innerStartCdcHttpSingleton: ActorRef = {
    Cluster(actorSystem).selfRoles.contains(AkkaCluster.MANAGER_ROLE) match {
      case true => actorSystem.actorOf(
        ClusterSingletonManager.props(
          singletonProps = CDCHttpFacadeActor.props,
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings(actorSystem).withRole(AkkaCluster.MANAGER_ROLE)),
        name = CDCShardExtension.CDC_HTTP_SERVICE)
      case _ => {}
    }
    actorSystem.actorOf(ClusterSingletonProxy.props(s"/user/${CDCShardExtension.CDC_HTTP_SERVICE}",
      ClusterSingletonProxySettings.apply(actorSystem).withRole(AkkaCluster.MANAGER_ROLE)),
      s"${CDCShardExtension.CDC_HTTP_SERVICE}-proxy")
  }

  /**
    * A set of internal DB queries used to persist incremental CDC Entity changes (aka deltas)
    * @param deltas
    * @return
    */
  protected[cdc] def insertDeltas(deltas: List[CdcDelta]): Future[Unit] =
    try {
      import cassContext._
      val fs = deltas
        .map(e => quote {
          query[CdcDelta].insert(lift(e))
        }).map(cassContext.prepareBatchAction(_))
      Future.sequence(fs).map(cassContext.executeBatchActions(_, Some(100)))
    } catch {
      case exc: Throwable => Future.failed(exc)
    }

  protected[cdc] def upsertFlushState(state: CdcFlushState): Future[Unit] =
    try {
      import cassContext._
      cassContext.run(quote{
        query[CdcFlushState].insert(lift(state))
      })
    } catch {
      case exc: Throwable => Future.failed(exc)
    }

  protected[cdc] def queryDeltas(shardId: String, bucket: Int, lastSeq: Int, currSeq: Option[Int] = None): Future[List[CdcDelta]] =

      try {
        import cassContext._

        def innerQueryDeltas(r: Range) =
          cassContext.run(quote {
            query[CdcDelta]
              .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket)))
              .filter(p => (p.seqNumber >= lift(r.start) && p.seqNumber <= lift(r.end)))
          })

        def innerQueryKeys =
          cassContext.run(quote {
            query[CdcDelta]
              .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket) && p.seqNumber > lift(lastSeq)))
              .map(_.seqNumber)
          })

        def innerQueryKeysWithLimit(limit:Int) =
          cassContext.run(quote {
            query[CdcDelta]
              .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket) && p.seqNumber > lift(lastSeq) && p.seqNumber <= lift(limit)))
              .map(_.seqNumber)
          })

        val f = currSeq match {
          case Some(l) => innerQueryKeysWithLimit(l)
          case _ => innerQueryKeys
        }

        f.flatMap(all => {
          val temp = all.foldLeft[(Seq[Range], Seq[Int])]((Seq(), Seq()))((x,s) => {
            val y = x._2 :+ s
            if (y.size == 100) {
              (x._1 :+ Range(y.head, y.last), Seq())
            } else {
              (x._1, y)
            }
          })
          val fmap = (temp._2.isEmpty match {
            case false => temp._1 :+ Range(temp._2.head, temp._2.last)
            case _ => temp._1
          }).map(r => innerQueryDeltas(r)).toList
          Future.sequence(fmap).map(_.flatMap(identity))
        })
      } catch {
        case exc: Throwable => Future.failed(exc)
      }


  protected[cdc] def queryFlushState(shardId: String): Future[List[CdcFlushState]] =
    try {
      import cassContext._
      cassContext.run(quote {
        query[CdcFlushState]
          .filter(p => (p.shardId == lift(shardId)))
      })
    } catch {
      case exc: Throwable => Future.failed(exc)
    }

  protected[cdc] def recoverDeltas(shardId: String): Future[Option[(Int,Int,Int)]] = {
    try {
      import cassContext._

      def getMaxDelta(bucket: Int, lastSeq: Int) =
        cassContext.run(quote {
          query[CdcDelta]
            .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket) && p.seqNumber > lift(lastSeq)))
            .map(_.seqNumber)
        })

      queryFlushState(shardId).flatMap(_.headOption match {
        case Some(x) => getMaxDelta(x.bucket, x.seqNumber).map(o => Some(x.bucket, x.seqNumber, o.lastOption.getOrElse(x.seqNumber)))
        case _ => Future.successful(None)
      })

    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

  protected[cdc] def deleteDeltas(shardId: String, bucket: Int): Future[Unit] = try {
    import cassContext._
    cassContext.run(quote {
      query[CdcDelta]
        .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket))).delete
    })
  } catch {
    case exc: Throwable => Future.failed(exc)
  }
}

case class CdcConfig(val period: Int, val shardNum: Int)

object CDCShardExtension

  extends ExtensionId[CDCShardExtensionImpl]
    with ExtensionIdProvider {

  type CDC[E<:CDCEntity] = CDCEntity with Message[E]
  type AnyC = GeneratedMessageCompanion[T] forSome {type T<:CDC[T]}

  lazy val config: Map[String, CdcConfig] = {
    import scala.collection.JavaConverters._
    AkkaCluster.configurationProvider.getConfig.getConfigList("cdc.entities").asScala
      .groupBy(_.getString("fqn")).mapValues(c => {
      val h = c.head
      CdcConfig(h.getInt("period"), h.getInt("shards"))
    })
  }

  lazy val cdcEc = AkkaCluster.actorSystem.dispatchers.lookup("dispatchers.cdc")

  val CDC_HTTP_SERVICE = "cdc-http-service"

  override def lookup = CDCShardExtension

  override def createExtension(system: ExtendedActorSystem) = new CDCShardExtensionImpl(system)

  override def get(system: ActorSystem): CDCShardExtensionImpl = super.get(system)

}

trait CDCShardedEntityOperation extends ShardedEntityOperation {

  override def getServiceName: String = "cdc"

  override def getServiceHandler(serviceActor: ActorRef): ShardedEntityHandler =  new CDCShardHandler(serviceActor)

}

/**
  * Three CDC-specific sharded service operations
  */
trait DbUpsert extends CDCShardedEntityOperation

trait CdcProcessingUpdate extends CDCShardedEntityOperation

trait CdcPassivate extends PassivateShardedEntityOperation with CDCShardedEntityOperation


case class CdcProcessingStatus(val shardId: String, bucket: Int, val id: String,  val processingState: Int)

case class CdcProcessingBucket(val shardId: String, val activeBucket: Int)

/**
  * This trait is used for entities with massive traffic so that micro-batched flush operations are used to persist in Cass
  * CDC processing status reflects the stage of CDC processor confirmation, basically, implementing AtLeastOnce ReliableDelivery persistence of in-flight messages
  * @tparam E
  */
trait DelayedCDCProcessingCompanion[E<:CDCEntity]{

  lazy val ctx: CDCCassandraContext = CDCShardExtension.get(AkkaCluster.actorSystem).cassContext

  def updateActiveCdcProcessingBucket(bucket: CdcProcessingBucket): Future[Unit] = {
    try {
      import ctx._
      ctx.run(quote {
        query[CdcProcessingBucket]
          .insert(lift(bucket))
      })
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

  def findActiveCdcProcessingBucket(shardId: String): Future[Int] = {
    try {
      import ctx._
      ctx.run(quote {
        query[CdcProcessingBucket]
          .filter(p => (p.shardId == lift(shardId)))
          .map(p => p.activeBucket)
      }).map[Int](_.headOption.getOrElse(1))
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

  def clearCdcProcessingBucket(shardId: String, bucket: Int): Future[Unit] = {
    try {
      import ctx._
      ctx.run(quote {
        query[CdcProcessingStatus]
          .filter(p => (p.shardId == lift(shardId) && p.bucket == lift(bucket))).delete
      })
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }


  def updateCdcProcessingStatus(entries: Seq[CdcProcessingStatus]): Future[Unit] = {
    try {
      import ctx._
      val fs = entries
        .map(e => quote {
          query[CdcProcessingStatus].insert(lift(e))
        }).map(ctx.prepareBatchAction(_))
      Future.sequence(fs).map(ctx.executeBatchActions(_, Some(200)))
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }


  def recoverCdcProcessingStatus(bucket: Int, shardId: String): Future[List[CdcProcessingStatus]] = {
    try {
      import ctx._
      ctx.run(quote {
        query[CdcProcessingStatus]
          .filter(p => (p.bucket == lift(bucket) && p.shardId == lift(shardId)))
      }).map(_.filter(_.processingState >= 0))
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

}

case class CdcDelta(val shardId: String, val bucket: Int, val seqNumber: Int, val blob: Array[Byte])

case class CdcFlushState(val shardId: String, val bucket: Int, val seqNumber: Int)

object CDCFlush {

  val flushTypeToken = new TypeToken[(CDCFlush[_],Cancellable)](){}

  val rnd = new Random()
}

/**
  * CDC Flush is used to aggregate and merge micro-batched deltas into the structure snapshot entity table
  * It also invokes CDC Processors
  * Note: Flushing is happening concurrently with new delta updates: no locking is needed as the flush method is thread-safe. Lots of thinking went into it
  * @param isRunning
  * @param parentRef
  * @param extension
  * @param shardId
  * @param currentShardInfo
  * @param lastSeq
  * @param period
  * @tparam E
  */
case class CDCFlush[E<:CDC[E]](val isRunning: AtomicBoolean,
                               val parentRef: ActorRef,
                               val extension: CDCShardExtensionImpl,
                               val shardId: String,
                               val currentShardInfo: CDCShardInfo[E],
                               val lastSeq: Int,
                               val period: Int) extends LazyLogging with ShardedEntityActorDispatchAction{

  override type T = (CDCFlush[_],Cancellable)

  override val typeToken =  CDCFlush.flushTypeToken

  private def isMapCompanion(cmp: GeneratedMessageCompanion[_]) = {
    val d = cmp.scalaDescriptor
    if (d.fields.size == 2) {
      (d.findFieldByNumber(1), d.findFieldByNumber(2)) match {
        case (Some(x), Some(y)) => x.name.contains("key") && y.name.contains("value")
        case _ => false
      }
    } else false
  }

  private def mergeField(old: Option[PValue], delta: PValue, field: FieldDescriptor, cmp: GeneratedMessageCompanion[_]): PValue =
    (delta, old) match {
      case (m: PRepeated, Some(l: PRepeated)) => {
        val combined = l.value ++ m.value
        combined.headOption match {
          case Some(PMessage(_)) => {
            val childCmp = cmp.messageCompanionForFieldNumber(field.number)
            if (isMapCompanion(childCmp)) {
              val d = childCmp.scalaDescriptor.findFieldByNumber(1).get
              val defaultChild = childCmp.defaultInstance.asInstanceOf[GeneratedMessage]
              PRepeated(combined.map(_.asInstanceOf[PMessage]).groupBy(_.value(d)).map(x => merge(List(defaultChild ->
                  x._2.map(p => childCmp.messageReads.read(p).asInstanceOf[GeneratedMessage]).toList), childCmp).head.toPMessage).toVector)
            } else PRepeated(combined)
          }
          case _ => PRepeated(combined)
        }
      }
      case (m: PMessage, Some(l: PMessage)) => {
        val childCmp = cmp.messageCompanionForFieldNumber(field.number)
        merge(List((childCmp.messageReads.read(l).asInstanceOf[GeneratedMessage] -> List(childCmp.messageReads.read(m).asInstanceOf[GeneratedMessage]))), childCmp).head.toPMessage
      }
      case (PEmpty, y) => y.getOrElse(PEmpty)
      case (x, _) => x
    }

  private def filterNonDefaultFieldValues(message: GeneratedMessage): Map[FieldDescriptor, PValue] = {
    val cmp = message.companion
    val d = cmp.scalaDescriptor
    val default = cmp.defaultInstance.asInstanceOf[GeneratedMessage]
    if (d.fullName.startsWith("google.protobuf") && d.fullName.endsWith("Value")) message.toPMessage.value.filterNot(_._2 == PEmpty)
      else message.toPMessage.value.filterNot(c => c._2 == default.getField(c._1))
  }

  private def merge(data: List[(GeneratedMessage, List[GeneratedMessage])], cmp: GeneratedMessageCompanion[_]): List[GeneratedMessage] = data.map(d =>
      cmp.messageReads.read(PMessage(d._2.foldLeft[Map[FieldDescriptor, PValue]](filterNonDefaultFieldValues(d._1))((x, y) =>
        filterNonDefaultFieldValues(y).foldLeft[Map[FieldDescriptor, PValue]](x)((v, w) =>
          v + (w._1 -> mergeField(v.get(w._1), w._2, w._1, cmp)))))).asInstanceOf[GeneratedMessage]
    )

  override def execute(prev: Option[Try[T]]) = {
    import scala.concurrent.duration._
    implicit val ec = CDCShardExtension.cdcEc
    if (isRunning.get()) {
      val cmp = currentShardInfo._1
      val ref = currentShardInfo._2.get()
      val currBucket = ref._1
      val latestSeq = ref._2.get()
      val tick = System.currentTimeMillis()
      logger.debug(s"Running CDC flush task for shard $shardId")
      val f1: Future[(Int,Set[String])] =  if ((latestSeq - lastSeq) <= 0) Future.successful(lastSeq -> Set()) else
        extension.queryDeltas(shardId, currBucket, lastSeq, Some(latestSeq)).flatMap[(Int, Set[String])](z => {
            val q = z.sortBy(_.seqNumber)
            logger.debug(s"Queried ${q.size} CDC deltas in shard $shardId, accumulated latency: ${System.currentTimeMillis() - tick}")
            val deltasExtended: Map[String, List[(E, Int)]] = q.map(d => (cmp.parseFrom(d.blob), d.seqNumber)).groupBy(_._1.getKey)
            val deltaOrdering: Map[String, Int] = deltasExtended.mapValues(_.maxBy(_._2)._2)
            val deltas = deltasExtended.mapValues(_.map(_._1))
            cmp.queryEntities(shardId, Some(deltas.keySet)).flatMap(w => {
              val oldEntities = w.groupBy(_.getKey).mapValues(_.head)
              logger.debug(s"Queried ${oldEntities.size} CDC entities in shard $shardId, accumulated latency: ${System.currentTimeMillis() - tick}")
              val newEntities = merge(deltas.keySet.map(k => (oldEntities.getOrElse(k, cmp.defaultInstance) -> deltas.getOrElse(k, List()))).toList, cmp)
               .map(x => {
                    val v = x.asInstanceOf[E]
                    v -> deltaOrdering.get(v.getKey).getOrElse(1)
                 }).sortBy(_._2).map(_._1)
              val s1 = cmp.processors.toList.map(_.processCDCEntities(newEntities))
              val f2 = cmp.upsertEntities(newEntities)
              val f1 = Future.sequence(s1)
              for {_ <- f1
                   _ <- f2} yield {
                  logger.info(s"Flushed ${newEntities.size} CDC entities in shard $shardId, accumulated latency: ${System.currentTimeMillis() - tick}")
                  if (q.isEmpty) latestSeq -> Set() else q.last.seqNumber -> newEntities.map(_.getKey).toSet
              }})
          }).flatMap[(Int,Int, Set[String])](s => if (s._1 > DELTA_BUCKET_SIZE) {
              val newCurrBucket = if (currBucket < DELTA_MAX_BUCKET_NUMBER) (currBucket + 1) else 1
              currentShardInfo._2.set(newCurrBucket -> new AtomicInteger(0))
              //sleep to make sure DB inserts that grabbed the old bucket - have finished writing
              Thread.sleep(500)
              extension.queryDeltas(shardId, currBucket, s._1).flatMap[(Int,Int, Set[String])](z1 => {
                val q1 = z1.sortBy(_.seqNumber)
                val newS = -1 * q1.size
                val counter = new AtomicInteger(newS)
                extension.insertDeltas(q1.map(x => CdcDelta(shardId, newCurrBucket, counter.incrementAndGet(), x.blob)))
                  .flatMap[(Int,Int, Set[String])](_ => extension.deleteDeltas(shardId, currBucket).map(_ => {
                  logger.info(s"Rolled a new CDC deltas bucket #$newCurrBucket with initial sequence #$newS for the shard $shardId")
                  (newCurrBucket, newS, s._2)
                }))
              }).recover{case x: Throwable => {
                logger.error(s"CDC delta bucket roll: Caught exception while running CDC flush task for shard $shardId: $x")
                (newCurrBucket,0, s._2)
              }}
            } else Future.successful((currBucket,s._1, s._2))
          ).flatMap[(Int,Set[String])](x => extension.upsertFlushState(CdcFlushState(shardId, x._1, x._2)).map(_ => x._2 -> x._3))
       val f2: Future[(Int, Set[String])] = currentShardInfo._3 match {
          case Some(st) => f1.flatMap[(Int, Set[String])](s => {
            val d = st._1
            val b = st._3.get
            st._4 ++= s._2.map(k => k -> new AtomicInteger(0))
            val stale = st._4.filter(_._2.get > 1).map(_._1).toSet
            val notProcessed = st._4.filter(_._2.get == 0).toSeq
            val processed = st._4.filter(_._2.get < 0).toSeq
            d.updateCdcProcessingStatus((Map[String, AtomicInteger]() ++ notProcessed ++ processed).toSeq.map(x => CdcProcessingStatus(shardId, b, x._1, x._2.get()))).flatMap[(Int, Set[String])](_ => {
              logger.info(s"Updated CDC processing status of ${processed.size} processed and ${notProcessed.size} in-process entries in shard $shardId, total latency ${System.currentTimeMillis() - tick}")
              st._4 --= processed.map(_._1)
              val remaining = st._4.filter(_._2.get >= 0)
              (if ((System.currentTimeMillis() - st._2.get()) > CDC_PROCESSING_BUCKET_INTERVAL_MILLIS) {
                val newB = if ((b + 1) > CDC_PROCESSING_MAX_BUCKET_NUMBER) 1 else (b + 1)
                d.updateActiveCdcProcessingBucket(CdcProcessingBucket(shardId, newB)).flatMap(_ =>
                  d.updateCdcProcessingStatus(remaining.toSeq.map(e => CdcProcessingStatus(shardId, newB, e._1, e._2.get()))).flatMap(_ =>
                    d.clearCdcProcessingBucket(shardId, b))).map(_ => {
                  logger.info(s"Rolled new CDC processing bucket $newB in shard $shardId with ${remaining.size} in-process entries, total latency ${System.currentTimeMillis() - tick}")
                  st._3.compareAndSet(b, newB)
                  st._2.set(System.currentTimeMillis())
                  s
                })
              } else Future.successful(s)).recover{case x: Throwable => {
                logger.error(s"CDC processing status bucket roll: Caught exception while running CDC flush task for shard $shardId: $x")
                s
              }}.map(y => {
                remaining.foreach(_._2.incrementAndGet())
                if (!stale.isEmpty) {
                  val reprocess = new ReprocessStaleCDCEntities[E](cmp, shardId, stale)
                  AkkaCluster.actorSystem.scheduler.scheduleOnce(1.seconds) {
                    reprocess.reprocess
                  }
                }
                y
              })
          }).recover{case x: Throwable => {
              logger.error(s"CDC processing status phase: Caught exception while running CDC flush task for shard $shardId: $x")
              s
            }}
          })
          case _ => f1
        }
      f2.recover{case x: Throwable => {
        logger.error(s"CDC delta merge phase: Caught exception while running CDC flush task for shard $shardId: $x")
        (lastSeq, Set())
      }}.map[T](y => {
        val flush = CDCFlush(this.isRunning, this.parentRef, this.extension,
          this.shardId, this.currentShardInfo, y._1, this.period)
        logger.debug(s"Finished CDC flush task for shard $shardId")
        (flush, AkkaCluster.actorSystem.scheduler.scheduleOnce(period.seconds)(parentRef.tell(flush, null)))
      })
     } else Future.failed[T](new IllegalStateException(s"Could not flush because the CDC actor has stopped for shard $shardId"))
  }
}

/**
  * Provides a reliable delivery retry logic for delated CDC processing use-case
  * Basically, if there were no CDC processor Ack for two flush cycles - it will retry the CDC Processor
  * @param cmp
  * @param shardId
  * @param stale
  * @param ec
  * @tparam E
  */
private class ReprocessStaleCDCEntities[E<:CDC[E]](val cmp: CDCEntityCompanion[E], val shardId: String, val stale: Set[String])(implicit ec: ExecutionContext) extends LazyLogging {

  def reprocess: Unit = {
    val tack = System.currentTimeMillis()
    cmp.queryEntities(shardId, Some(stale))
      .flatMap(l => if (l.isEmpty) Future.successful() else {
        val fmap = cmp.processors.map(_.processCDCEntities(l))
        Future.sequence(fmap)
      }).onComplete(_ match {
          case Failure(x) =>  logger.error(s"Caught exception while reprocessing ${stale.size} stale CDC entities for shard $shardId: $x")
          case _ => logger.info(s"Reprocessed ${stale.size} CDC entities in shard $shardId, total latency ${System.currentTimeMillis() - tack}")
    })
  }
}

case object ForceCDCFlushOperation extends CDCShardedEntityOperation

/**
  * Forces an immediate Flush (without waiting for the next scheduled flush)
  * Think of the cost of flushing immediately - so don't abuse. Use-case is user-facing Swagger update API
  */
trait ForceCDCFlush extends LazyLogging with ShardedEntity with ShardedEntityActorDispatchAction{


  override type T = (CDCFlush[_],Cancellable)

  override val typeToken =  CDCFlush.flushTypeToken


  override def getOperation: ShardedEntityOperation = ForceCDCFlushOperation

  override def setOperation(operation: ShardedEntityOperation) = this

  override def execute(prev: Option[Try[T]]) = {
    implicit val tag = ClassTag[T](typeToken.getRawType)
    prev match {
      case Some(Success(x: T)) => {
        x._2.cancel()
        logger.info(s"Cancelled CDC flush task in shard ${x._1.shardId}: status ${x._2.isCancelled}")
        val result = x._1.execute(prev)
        logger.info(s"Forced immediate CDC flush in shard ${x._1.shardId}: result is ${result}")
        result
      }
      case _ => Future.failed[T](new IllegalStateException(s"Could not force flush the CDC actor"))

    }
  }

}

/**
  * CDC sharing service handler
  * @param parentRef
  */
class CDCShardHandler(override protected val parentRef:ActorRef) extends BaseShardedEntityHandler with LazyLogging{

  import collection.JavaConverters._

  lazy val ext = CDCShardExtension.get(AkkaCluster.actorSystem)

  parentRef ! ShardedEntityActorOnStop(() => this.terminate())

  val isFlushRunning: AtomicBoolean = new AtomicBoolean(true)

  val shards = new ConcurrentHashMap[String,CDCShardInfo[_]]()

  val shardInitializers = TrieMap[String, CDCShardInitializer[_]]()

  implicit lazy val ec = CDCShardExtension.cdcEc

  class CDCShardInitializer[E<:CDC[E]](clazz: Class[E], cmp: CDCEntityCompanion[E]) extends java.util.function.Function[String, CDCShardInfo[_]]{

    /**
      * Encapsulates all recovery logic of the CDC shard state when a shard re-starts
      * @param shardId
      * @return
      */
    override def apply(shardId: String): CDCShardInfo[_] = {
      import scala.concurrent.duration._
      Try[CDCShardInfo[_]]{
        val tick = System.currentTimeMillis()
        val lastDelta = Await.result(ext.recoverDeltas(shardId), 300.seconds).getOrElse((1, 0, 0))
        logger.info(s"Recovered latest CDC delta with bucket ${lastDelta._1} and sequence ${lastDelta._2} in shard ${shardId}, latency: ${System.currentTimeMillis() - tick}")
        val cdcProcessingState: Option[CDCProcessingInfo[E]] = cmp match {
          case d: DelayedCDCProcessingCompanion[E] => {
            val b = Await.result(d.findActiveCdcProcessingBucket(shardId), 30.seconds)
            val l = Await.result(d.recoverCdcProcessingStatus(b, shardId), 30.seconds)
            logger.info(s"Recovered latest CDC processing states of size ${l.size} with bucket $b in shard ${shardId}, latency: ${System.currentTimeMillis() - tick}")
            Some(d, new AtomicLong(System.currentTimeMillis()), new AtomicInteger(b), TrieMap(l.map(s => s.id -> new AtomicInteger(s.processingState + 1)): _*))
          }
          case _ => None
        }
        val shardInfo: CDCShardInfo[E] = (cmp, new AtomicReference(lastDelta._1, new AtomicInteger(lastDelta._3)), cdcProcessingState)
        val period = CDCShardExtension.config(clazz.getName).period
        val half = Math.ceil(period / 2).toInt
        val flush = CDCFlush[E](isFlushRunning, parentRef, ext, shardId,
          shardInfo, lastDelta._2, period)
        AkkaCluster.actorSystem.scheduler.scheduleOnce((period + CDCFlush.rnd.nextInt(period) - half).seconds)(parentRef.tell(flush, null))
        shardInfo.asInstanceOf[CDCShardInfo[_]]
      }.recover{case x: Throwable => {
        onCdcShardStartFailure(shardId, x)
        (cmp, new AtomicReference(0, new AtomicInteger(0)), Option.empty[CDCProcessingInfo[E]]).asInstanceOf[CDCShardInfo[_]]
      }}.get
    }
  }

  private def onCdcShardStartFailure(shard: String, exc: Throwable) = {
    logger.error(s"Could not start CDC shard ${shard}, stopping it: $exc")
    parentRef ! PoisonPill
  }

  private def resolveCDCEntities[E<:ShardedEntity](e: E): Seq[CDCEntity] = e.companion.asInstanceOf[CDCEntityResolver[E]].resolveCDCEntities(e)

  /**
    * Main inner DB transaction method of the CDC handler
    * Is thread-safe and is running in high-concurrency. Future composition is used to avoid blocking: lots of thinking went into it
    * @param entities
    * @param operation
    * @param correlationId
    * @return
    */
  private def persistCDCEntities(entities: Seq[CDCEntity], operation: ShardedEntityOperation, correlationId: Option[String]): Future[Option[AckMessage]] =
    entities.headOption match {
      case Some(head) => operation match {
          case _: DbUpsert =>  (Future{
            val shardInfo = getOrUpdateShardInfo(head.reify)
            val ref = shardInfo._2.get()
            val deltas = entities.filterNot(e => Strings.isNullOrEmpty(e.getKey)).map(e => CdcDelta(e.getShardID, ref._1, ref._2.incrementAndGet(), e.toByteArray)).toList
            val p = Promise[Unit]()
            p.completeWith(deltas.isEmpty match {
              case false => ext.insertDeltas(deltas)
              case _ => Future.successful[Unit]()
            })
            p.future
          }).flatMap(_.map(_ => correlationId match {
            case Some(i) => Some(Ack.ack.withCorrelationId(i))
            case _ => Some(Ack.ack)
          }))

          case _: CdcProcessingUpdate => Future{
            val shardInfo = getOrUpdateShardInfo(head.reify)
            shardInfo._3.foreach(s => entities.filterNot(e => Strings.isNullOrEmpty(e.getKey)).foreach(x => {
                s._4.replace(x.getKey, new AtomicInteger(-1))
                s._4.putIfAbsent(x.getKey, new AtomicInteger(0))
              }))
              None
          }

          case _ => Future{
            throw new UnsupportedOperationException(s"Operation ${operation} is not supported")
          }
      }

      case _ => Future.successful(None)
    }



  def getOrUpdateShardInfo[E<:CDC[E]](entity: E): CDCShardInfo[E] = {
    val initializer = shardInitializers.getOrElseUpdate(entity.getShardID,
      new CDCShardInitializer[E](entity.getClass.asInstanceOf[Class[E]],  entity.cdcCompanion.asInstanceOf[CDCEntityCompanion[E]]))
    shards.computeIfAbsent(entity.getShardID, initializer).asInstanceOf[CDCShardInfo[E]]
  }

  override def innerHandle(message: ShardedEntity) = {
    persistCDCEntities(resolveCDCEntities(message), message.getOperation, message match {
      case m: AsyncAskMessage[_] => Some(m.correlationId)
      case _ => None
    })
  }

  private def terminate(): Unit = {isFlushRunning.set(false)}
}

