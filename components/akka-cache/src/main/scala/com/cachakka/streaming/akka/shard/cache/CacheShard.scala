package com.cachakka.streaming.akka.shard.cache

import java.util.{Base64, Date, Optional, Random}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, PoisonPill}
import akka.cluster.Cluster
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.management.cluster.ClusterReadViewAccess
import akka.pattern.Patterns
import com.google.common.base.{CharMatcher, Preconditions, Strings}
import com.google.common.reflect.TypeToken
import com.google.inject.Key
import com.google.inject.name.Names
import com.google.protobuf.ByteString
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.akka._
import com.cachakka.streaming.akka.shard.cdc.CDCShardExtension.{AnyC, CDC}
import com.cachakka.streaming.akka.shard.cdc._
import org.joda.time.DateTime
import org.xerial.snappy.Snappy

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scalapb.descriptors.{PValue, _}


/**
  *
  * Akka-Cache extension (loaded together with the actor system)
  * @param actorSystem
  */
class CacheShardExtensionImpl(actorSystem: ActorSystem) extends Extension{


  private val replicationServiceInstanceCount = CacheShardExtension.config.foldLeft[Int](0)((x,s) => Math.max(x, (s._2.replication-1)))

  val cacheShardRegistrySingletonActor: AtomicReference[ActorRef] = new AtomicReference()

  /**
    * Replication sharded service: only created if replication factor is strictly greater than 1. Ensures that replicas run on different nodes
    * Replication is optional: not used in recovery (Cass is used to recover from)
    */
  val cacheReplicaServiceShardRegions: AtomicReference[Seq[ActorRef]] = {
    val r =  new AtomicReference[Seq[ActorRef]]()
    AkkaCluster.onMemberUpCallbacks += 15 -> (() => r.set(innerStartCacheReplicaShardingRegions))
    AkkaCluster.onMemberUpCallbacks += 5 -> (() => cacheShardRegistrySingletonActor.set(innerStartCacheShardRegistrySingleton))
    r
  }

  protected def innerStartCacheReplicaShardingRegions: Seq[ActorRef] = Range(0, replicationServiceInstanceCount)
    .map[ActorRef, Seq[ActorRef]](i => ClusterSharding.get(actorSystem).start(
      s"${CacheShardExtension.CACHE_REPLICA_SERVICE}-$i", ShardedEntityActor.props, ClusterShardingSettings.create(actorSystem),
      AkkaCluster.shardingMessageExtractor, AkkaCluster.shardAllocationStrategy, PoisonPill))


  protected def innerStartCacheShardRegistrySingleton: ActorRef = {
    Cluster(actorSystem).selfRoles.contains(AkkaCluster.MANAGER_ROLE) match {
      case true => actorSystem.actorOf(
        ClusterSingletonManager.props(
          singletonProps = CacheShardRegistry.props,
          terminationMessage = PoisonPill,
          settings = ClusterSingletonManagerSettings(actorSystem).withRole(AkkaCluster.MANAGER_ROLE)),
        name = CacheShardExtension.CACHE_SHARD_REGISTRY)
      case _ => {}
    }
    actorSystem.actorOf(ClusterSingletonProxy.props(s"/user/${CacheShardExtension.CACHE_SHARD_REGISTRY}",
      ClusterSingletonProxySettings.apply(actorSystem).withRole(AkkaCluster.MANAGER_ROLE)),
      s"${CacheShardExtension.CACHE_SHARD_REGISTRY}-proxy")
  }

  def messageShardRegistry(m: CacheRegistryMessage)(implicit sender: ActorRef): Unit = {
    cacheShardRegistrySingletonActor.get() ! m
  }

  def asyncMonitorShardRegistry[T<: CacheRegistryMonitoringMessage[_]](m: T, timeout: FiniteDuration = 10.seconds): Future[T] = {
    implicit lazy val ec = AkkaCluster.miscEc
    Patterns.ask(cacheShardRegistrySingletonActor.get(), m, timeout).map(_.asInstanceOf[T])
  }

  def syncCallReplicaService(m: ShardedEntity, timeout: FiniteDuration = 10.seconds, replicaID: Int = 0): Try[ClusterMessage] = {
    Try{Await.result(asyncCallReplicaService(m, timeout, replicaID), timeout)}
  }

  def asyncCallReplicaService(m: ShardedEntity, timeout: FiniteDuration = 10.seconds, replicaID: Int = 0): Future[ClusterMessage] = {
    implicit lazy val ec = AkkaCluster.miscEc
    Patterns.ask(CacheShardExtension.get(AkkaCluster.actorSystem).cacheReplicaServiceShardRegions.get().apply(replicaID), m, timeout)
      .map(_.asInstanceOf[ClusterMessage])
  }

  def messageReplicaService(m: ShardedEntity, replicaID: Int = 0)(implicit sender: ActorRef): Unit = {
    CacheShardExtension.get(AkkaCluster.actorSystem).cacheReplicaServiceShardRegions.get().apply(replicaID) ! m
  }

}

case class CacheConfig(
  val replication: Int,
  val pollingPeriod: Int,
  val demandRate: Double,
  val shardsRate: Double,
  val flushPeriod: Int,
  val retryPeriod: Int,
  val retryThreshold: Int,
  val shardNum: Int
)

object CacheShardExtension
  extends ExtensionId[CacheShardExtensionImpl]
    with ExtensionIdProvider {

  val CACHE_REPLICA_SERVICE = "cache-replica-service"

  val CACHE_SHARD_REGISTRY = "cache-shard-registry"

  val CACHE_BUCKET_MANAGER = "cache-bucket-manager"

  lazy val cacheEc = AkkaCluster.actorSystem.dispatchers.lookup("dispatchers.cache")

  lazy val config: Map[String, CacheConfig] = {
    import scala.collection.JavaConverters._
    AkkaCluster.configurationProvider.getConfig.getConfigList("cache.entities").asScala
      .groupBy(_.getString("fqn")).map(e  => {
      val h = e._2.head
      val flushPeriod = Math.max(CDCShardExtension.config(e._1).period - 20, Math.min(10, CDCShardExtension.config(e._1).period - 1))
      e._1 -> CacheConfig(h.getInt("replication"), h.getInt("period"), h.getDouble("rate.demand"), h.getDouble("rate.shards"), flushPeriod,
        Try{h.getInt("retry.period")}.getOrElse(Integer.MAX_VALUE), Try{h.getInt("retry.threshold")}.getOrElse(Integer.MAX_VALUE), h.getInt("shards"))
    })
  }

  val CACHE_QUERY_MAX_RESULT_SET_SIZE = 5000

  val CACHE_DIGEST_QUERY_MAX_RESULT_SET_SIZE = 20000

  val CACHE_FLUSH_MAX_PACKET_SIZE = 1000

  val CACHE_COLLECT_RETRY_MAX = 10

  val CACHE_RETRY_MAX_SET_SIZE = 100

  val CACHE_COLLECT_RETRY_PAUSE = 2.millis

  def getReplicationFactor[E<:CDCEntity](implicit ct: ClassTag[E]) = config.get(ct.runtimeClass.getName).map(_.replication).getOrElse(1)

  override def lookup = CacheShardExtension

  override def createExtension(system: ExtendedActorSystem) = new CacheShardExtensionImpl(system)

  override def get(system: ActorSystem): CacheShardExtensionImpl = super.get(system)

}

/**
  * This trait provides persistence of cache indexes in Cass
  * It is needed only for huge cache sizes: otherwise, the CDC table is sufficient
  * @tparam E
  */
trait SnapshottedCacheEntityCompanion[E<:CDC[E]] extends DelayedCDCProcessingCompanion[E] with LazyLogging{

  self: CDCEntityCompanion[E] =>

  def upsertToSnapshot(entries: Seq[CacheIndexSnapshot]): Future[Unit] = {
    try {
      import ctx._
      val fs = entries
        .map(e => quote {
          query[CacheIndexSnapshot].insert(lift(e))
        }).map(ctx.prepareBatchAction(_))
      Future.sequence(fs).map(ctx.executeBatchActions(_, Some(50)))
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

  def queryFromSnapshot(shardId: String, indexId: String) = {
    try {
      import ctx._

      def innerQueryCapture(keys: Set[String]) =
        ctx.run(quote {
          query[CacheIndexSnapshot]
            .filter(p => (p.cacheShard == lift(shardId) && p.cacheIndex == lift(indexId)))
            .filter(p => liftQuery(keys).contains(p.id))
        })

      ctx.run(quote {
        query[CacheIndexSnapshot]
          .filter(p => (p.cacheShard == lift(shardId) && p.cacheIndex == lift(indexId)))
          .map(_.id)
      }).flatMap(all => {
        val temp = all.foldLeft[(Seq[Seq[String]], Seq[String])]((Seq(), Seq()))((x,s) => {
          val y = x._2 :+ s
          if (y.size == 50) {
            (x._1 :+ y, Seq())
          } else {
            (x._1, y)
          }
        })
        val fmap = (temp._2.isEmpty match {
          case false => temp._1 :+ temp._2
          case _ => temp._1
        }).map(l => innerQueryCapture(l.toSet)).toList
        Future.sequence(fmap).map(_.flatMap(identity))
      })
    } catch {
      case exc: Throwable => Future.failed(exc)
    }
  }

  def deleteFromSnapshot(shardId: String, indexId: String, keys: Set[String]): Future[Unit] =  try {
    import ctx._
    ctx.run(quote {
      query[CacheIndexSnapshot]
        .filter(p => (p.cacheShard == lift(shardId) && p.cacheIndex == lift(indexId)))
        .filter(p => liftQuery(keys).contains(p.id))
        .delete
    }).map(_ => logger.info(s"Deleted ${keys.size} cache index snapshot entries for index $indexId from shard $shardId"))
  } catch {
    case exc: Throwable => Future.failed(exc)
  }

}

case class CacheIndexPayload(val indexValues: Seq[Option[Array[Byte]]]) extends io.getquill.context.cassandra.Udt

case class CacheIndexSnapshot(val cacheShard: String, val cacheIndex: String, val id: String, val payload: Option[CacheIndexPayload])

/**
  * Implementation of the CDC Processor that forwards the current entity snapshot to a cache shard (in bulk)
  * Note: it is a fire-and-forget implementation that relies on the underlying reliably delivery with status tracking mechanism
  * @tparam E
  */
class CacheForwardedCDCProcessor[E<:CDCEntity] extends CDCEntityProcessor[E] {

  implicit lazy val ec = AkkaCluster.miscEc

  protected val innerTransform: Iterable[E] => (Iterable[E],Iterable[E]) = {entities: Iterable[E] => {
    val default = entities.head.companion.defaultInstance.asInstanceOf[E]
    val successes = entities
      .map(x => Try {
        val y = x match {
          case v: AutoShardedEntity[E] => v.asInstanceOf[AutoShardedEntity[E]].processShardId(CacheUpdate)
          case z => z
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(y.getShardID))
        y
      })
      .filter(_.isSuccess)
      .map(_.get)
    val successKeys = successes.map(_.getKey).toSet
    val failures = entities.head.companion match {
      case d: DelayedCDCProcessingCompanion[_] => {
        entities.filterNot(z => successKeys.contains(z.getKey))
          .map(x => Try {
            val y = x match {
              case v: AutoShardedEntity[E] => v.asInstanceOf[AutoShardedEntity[E]].processShardId(CdcProcessingUpdate)
              case z => z
            }
            Preconditions.checkState(!Strings.isNullOrEmpty(y.getShardID))
            y
          })
          .filter(_.isSuccess)
          .map(z => {
            val e = z.get
            default.setKey(e.getKey).setShardID(e.getShardID).asInstanceOf[E]
          })
      }
      case _ => Iterable()
    }
    (successes, failures)
  }}

  override def processCDCEntities(entities: Iterable[E]) = {
    if (!entities.isEmpty) {
      implicit val sender = Actor.noSender
      val head = entities.head
      val cmp = head.setOperation(CdcProcessingUpdate).companion
      val resolver = cmp.asInstanceOf[CDCEntityResolver[_]]
      val defaultCacheUpdate: Option[resolver.DT] = Some(cmp.defaultInstance.asInstanceOf[ShardedEntity]
        .setOperation(CacheUpdate).asInstanceOf[resolver.DT])
      val defaultCdcUpdate: Option[resolver.DT] = Some(cmp.defaultInstance.asInstanceOf[ShardedEntity]
        .setOperation(CdcProcessingUpdate).asInstanceOf[resolver.DT])
      val ext = CacheShardExtension.get(AkkaCluster.actorSystem)
      val replicationFactor: Int = CacheShardExtension.getReplicationFactor(ClassTag(head.getClass))
      val rng = Range(0, replicationFactor - 1)
      val threshold = CacheShardExtension.CACHE_FLUSH_MAX_PACKET_SIZE
      val v = innerTransform(entities)
      v._1.groupBy(_.getShardID).values.toSeq.flatMap(x => {
        val z = x.foldLeft[(Seq[ShardedEntity], Seq[CDCEntity])]((Seq(), Seq()))((s, y) => {
          val next = s._2 :+ y
          (next.size == threshold) match {
            case true => {
              val w = resolver.wrapCDCEntities(next.map(_.asInstanceOf[resolver.CD]), defaultCacheUpdate).asInstanceOf[ShardedEntity]
              (s._1 :+ w, Seq())
            }
            case _ => (s._1, next)
          }
        })
        if (z._2.isEmpty) z._1 else {
          z._1 :+ resolver.wrapCDCEntities(z._2.map(_.asInstanceOf[resolver.CD]), defaultCacheUpdate).asInstanceOf[ShardedEntity]
        }
      })
      .foreach(m => {
        AkkaCluster.messageShardingService(m)
        rng.foreach(ext.messageReplicaService(m, _))
      })
      v._2.groupBy(_.getShardID).values.toSeq.flatMap(x => {
        val z = x.foldLeft[(Seq[ShardedEntity], Seq[CDCEntity])]((Seq(), Seq()))((s, y) => {
          val next = s._2 :+ y
          (next.size == threshold) match {
            case true => {
              val w = resolver.wrapCDCEntities(next.map(_.asInstanceOf[resolver.CD]), defaultCdcUpdate).asInstanceOf[ShardedEntity]
              (s._1 :+ w, Seq())
            }
            case _ => (s._1, next)
          }
        })
        if (z._2.isEmpty) z._1 else {
          z._1 :+ resolver.wrapCDCEntities(z._2.map(_.asInstanceOf[resolver.CD]), defaultCdcUpdate).asInstanceOf[ShardedEntity]
        }
      })
      .foreach(m => {
        AkkaCluster.messageShardingService(m)
      })
    }
    Future.successful({})
  }
}

/**
  * Replicated version of the cache-forwarder: broadcasts entities to all cache shards (so each is a full replica)
  * Can only be used for small cache sizes
  * @tparam E
  */
class ReplicatedCacheForwardedCDCProcessor[E<:CDCEntity] extends CacheForwardedCDCProcessor[E] {

  override protected val innerTransform = {entities: Iterable[E] => {
    val hosts = ClusterReadViewAccess.internalReacView(Cluster(AkkaCluster.actorSystem))
      .state.members.filter(_.roles.contains(AkkaCluster.WORKER_ROLE)).map(_.address.host.getOrElse(AkkaCluster.selfAddress.host.get))
    (entities.flatMap(x => hosts.map(h => x.setShardID(s"$CACHE_SHARD_ID_SIGNATURE$h").asInstanceOf[E])), Iterable())
  }}
}


class CachedCDCEntityProcessorProvider[E<:CDCEntity] extends CDCProcessorProvider[E] {

  override val processors = Iterable[CDCEntityProcessor[E]](new CacheForwardedCDCProcessor[E])

}

class ReplicatedCachedCDCEntityProcessorProvider[E<:CDCEntity] extends CDCProcessorProvider[E] {

  override val processors = Iterable[CDCEntityProcessor[E]](new ReplicatedCacheForwardedCDCProcessor[E])

}

/**
  * Cache storage compression implementation (say, Snappy or LZ4)
  * Crucial to reduce cache index RAM footprint
  * @tparam E
  * @tparam C
  */
trait CachedEntityCompactor[E<:CDCEntity, C] {

  def deflate(entity: E): C

  def inflate(value: C): E

}

class TrivialCachedEntityCompactor[E<:CDCEntity] extends CachedEntityCompactor[E,E]{

  override def deflate(entity: E) = entity

  def inflate(value: E) = value

}

trait ProtoCachedEntityCompactor[E<:CDC[E]] extends CachedEntityCompactor[E, Array[Byte]]{

  cmp: CDCEntityCompanion[E] =>

  override def deflate(entity: E) = entity.toByteArray

  override def inflate(value: Array[Byte]) = cmp.parseFrom(value)

}

trait SnappyCachedEntityCompactor[E<:CDC[E]] extends CachedEntityCompactor[E, Array[Byte]]{

  cmp: CDCEntityCompanion[E] =>

  override def deflate(entity: E) = Snappy.compress(entity.toByteArray)

  override def inflate(value: Array[Byte]) = cmp.parseFrom(Snappy.uncompress(value))

}


trait CacheUpdate extends CacheShardedEntityOperation
trait CacheGet extends CacheShardedEntityOperation
trait CacheGroupInitialize extends EncodedKeyCacheShardEntityOperation
trait CacheGroupStart extends EncodedKeyCacheShardEntityOperation
trait CacheGroupStop extends EncodedKeyCacheShardEntityOperation

trait CacheGroupPoll extends EncodedKeyCacheShardEntityOperation {

  def response[E<:CDCEntity](header: ShardedEntity, values: Seq[E]): ShardedEntity = {
    val resolver = getResolver(header)
    resolver.wrapCDCEntities(values.map(_.asInstanceOf[resolver.CD]), Some(header))
  }

  private def getResolver[T<:ShardedEntity](message: T) = message.companion.asInstanceOf[CDCEntityResolver[T]]

}

trait CacheQuery extends EncodedKeyCacheShardEntityOperation {

  def results[E<:CDCEntity](message: ShardedEntity): Seq[E] = {
    message.companion.asInstanceOf[CDCEntityResolver[message.type]].resolveCDCEntities(message).map(_.asInstanceOf[E])
  }

  /**
    * Cache query API
    * @param shardId
    * @param andPredicates
    * @param orPredicates
    * @param digestFields
    * @param cmp
    * @tparam E
    * @return
    */
  def request[E<:CDC[E]](shardId: String, andPredicates: Seq[(String, String)] = Seq(), orPredicates: Seq[(String, String)] = Seq(), digestFields: Option[Set[String]] = None)(implicit cmp: GeneratedMessageCompanion[E]) = {
    val m = andPredicates.zipWithIndex.map(x => s"${x._2}$AND_PREDICATE${x._1._2}" -> s"${x._1._1}") ++
      orPredicates.zipWithIndex.map(x => s"${x._2}$OR_PREDICATE${x._1._2}" -> s"${x._1._1}") ++
      digestFields.map(_.toSeq.zipWithIndex.map(x => s"${x._2}$DIGEST_FIELD" -> s"${x._1}")).getOrElse(Seq())
    header(shardId)(m.toArray:_*)
  }
}

/**
  * Main UDF feature to support secondary cache indexes
  * @tparam E
  */
trait CacheQueryFunction[E<:CDCEntity] {

  type R

  trait CacheQueryOperator {

    def generateValueRange(arg: String): Seq[R]

  }

  val supportedOperators: Map[String, CacheQueryOperator]

  def apply(input: E): Seq[R]

  def serialize(value: R): Array[Byte]

  def deserialize(blob: Array[Byte]): R

}


trait CacheQueryFunctionsProvider[E<:CDCEntity] {

  val supportedOperatorNames: Set[String]

  /**
    * input here is a string of parameters that allow to use generic UDFs that are tailored to specific fields by passing the parameter string
    * @param input
    * @return
    */
  def apply(input: String): CacheQueryFunction[E]

}

private[cache] class CacheQueryFunctionImpl[E<:CDCEntity,T](val pred: E => Seq[T], val operMap: Map[String, String => Seq[T]], val serDe: (T => Array[Byte], Array[Byte] => T)) extends CacheQueryFunction[E] {

  override type R = T

  override def apply(v: E) = pred(v)

  override val supportedOperators: Map[String, CacheQueryOperator] = operMap.mapValues(x => new CacheQueryOperator(){
    override def generateValueRange(arg: String): Seq[T] = x(arg)
  })

  override def serialize(value: T): Array[Byte] = serDe._1.apply(value)

  override def deserialize(blob: Array[Byte]): T = serDe._2.apply(blob)
}

trait ParameterizedCacheQueryFunctionsProvider[E<:CDCEntity,T] extends CacheQueryFunctionsProvider[E]{

  protected def innerOperMap(param: String): Map[String, String => Seq[T]]

  protected def innerFunction(param: String)(v: E): Seq[T]

  protected def innerSerDe(param: String):  (T => Array[Byte], Array[Byte] => T)

  override def apply(v: String): CacheQueryFunction[E] = new CacheQueryFunctionImpl[E,T](innerFunction(v), innerOperMap(v), innerSerDe(v))
}

/**
  * One of the most useful UDFs: extracts arbitrarily-nested field using full path and dotted notation.
  * Supports collection
  * Relies on Protobuf schema: uses Proto field descriptor names in the path
  * Only supports Equality operator (can be extended to support IN/Set-like operator)
  * @param cmp
  * @tparam E
  */
class CompositeFieldValueCacheQueryFunctionProvider[E<:CDC[E]](cmp: GeneratedMessageCompanion[E]) extends ParameterizedCacheQueryFunctionsProvider[E,PValue] {

  override val supportedOperatorNames: Set[String] = Set(":")

  val ser: PValue => Array[Byte] = { v =>
    Snappy.compress(v match {
      case PInt(x) => x.toString
      case PLong(x) => x.toString
      case PDouble(x) => x.toString
      case PFloat(x) => x.toString
      case PBoolean(x) => x.toString
      case PString(x) => x.toString
      case PByteString(x) => Base64.getEncoder.encodeToString(x.toByteArray)
      case PEnum(d) => d.number.toString
      case _ => "PEmpty"
    })
  }

  def checkIfDefault(v: PValue): PValue = v match {
    case PInt(x) => if (x == 0) PEmpty else v
    case PLong(x) => if (x == 0) PEmpty else v
    case PDouble(x) => if (x == 0.0) PEmpty else v
    case PFloat(x) => if (x == 0.0) PEmpty else v
    case PBoolean(x) => if (!x) PEmpty else v
    case PByteString(x) => if (x.isEmpty) PEmpty else v
    case PString(x) => if (Strings.isNullOrEmpty(x)) PEmpty else v
    case PEnum(d) => if (d.number == 0) PEmpty else v
    case _ => PEmpty
  }

  override protected def innerFunction(param: String)(v: E): Seq[PValue] = {
    val split = param.split("\\.").toSeq
    Try {
      split.foldLeft[Seq[(_, _)]](Seq((v.toPMessage, v)))((y, s) => y.flatMap(_ match {
        case (PEmpty, z) => Seq(PEmpty -> z)
        case (_, x: GeneratedMessage) => {
          val cmp = x.companion
          val f = cmp.scalaDescriptor.findFieldByName(s).get
          val childCmp = if (f.scalaType.isInstanceOf[ScalaType.Message]) cmp.messageCompanionForFieldNumber(f.number) else cmp
          x.toPMessage.value.getOrElse(f, PEmpty) match {
            case PRepeated(c) if c.isEmpty => Seq(PEmpty -> x)
            case PRepeated(c) => if (f.scalaType.isInstanceOf[ScalaType.Message]) c.map(l => l -> childCmp.messageReads.read(l))
            else c.map(l => checkIfDefault(l) -> x)
            case z: PMessage => Seq(z -> childCmp.messageReads.read(z))
            case w => Seq(checkIfDefault(w) -> x)
          }
        }
      }))
    }.getOrElse(Seq(PEmpty -> v)).map(_._1.asInstanceOf[PValue])
  }

  private def innerEq(param: String): String => Seq[PValue] = {
    val split = param.split("\\.")
    val t = Try {
      split.foldLeft[(_, _)]((null, cmp))((y, s) => {
        val x = y._2.asInstanceOf[GeneratedMessageCompanion[_]]
        val f = x.scalaDescriptor.findFieldByName(s).get
        val childCmp = if (f.scalaType.isInstanceOf[ScalaType.Message]) x.messageCompanionForFieldNumber(f.number) else x
        (f, childCmp.asInstanceOf[Any])
      })
    }.flatMap(z => Try {
      z._1.asInstanceOf[FieldDescriptor].scalaType
    })
    val r: String => Seq[PValue] = { x: String =>
      Seq((t, x.toLowerCase) match {
        case (_, "") | (_, "null") | (_, "empty") | (_, "none") | (_, "nil") => PEmpty
        case (Success(ScalaType.Int), _) => PInt(x.toInt)
        case (Success(ScalaType.Long), _) => PLong(x.toLong)
        case (Success(ScalaType.Double), _) => PDouble(x.toDouble)
        case (Success(ScalaType.Float), _) => PFloat(x.toFloat)
        case (Success(ScalaType.Boolean), _) => PBoolean(x.toBoolean)
        case (Success(ScalaType.ByteString), _) => PByteString(ByteString.copyFrom(Base64.getDecoder.decode(x)))
        case (Success(ScalaType.String), _) => PString(x)
        case (Success(ScalaType.Enum(d)), _) => Try {
          x.toInt
        } match {
          case Success(z) => PEnum(d.values.find(_.number == z).getOrElse(d.values(0)))
          case _ => PEnum(d.values.find(_.name == x).getOrElse(d.values(0)))
        }
        case _ => PEmpty
      })
    }
    r
  }


  override protected def innerOperMap(param: String) = Map(":" -> innerEq(param))

  override protected def innerSerDe(param: String): (PValue => Array[Byte], Array[Byte] => PValue) = {
    val innerDes = innerEq(param)
    val des: Array[Byte] => PValue = { v => innerDes(Snappy.uncompressString(v)).head }
    ser -> des
  }

}

/**
  * Another useful UDF family: filter by timestamp of a particular CDCAgent (agent name is passed as the parameter string)
  * Supports Eq, Geq and Leq operators
  * Note: uses timestamp rounding to integer number of interval increments where interval is XX minutes (XX must be a divisor of 60)
  * @tparam E
  */
trait TimestampCacheQueryFunctionProvider[E<:CDC[E]]  extends ParameterizedCacheQueryFunctionsProvider[E, Int]{

  protected val intervalMinutes: Int
  protected val maxDurationMinutes: Int

  override val supportedOperatorNames: Set[String] = Set(":", ">", "<")

  protected def roundDate(millis: Long, minutes: Int) = {
    if (minutes < 1 || 60 % minutes != 0) throw new IllegalArgumentException("minutes must be a factor of 60")
    Math.round(millis / (60000.0 * minutes)).toInt
  }

  private def innerEq(x: String): Seq[Int] = x.toLowerCase match {
    case ""|"null"|"empty"|"none"|"nil" => Seq(0)
    case _ => Seq(roundDate((System.currentTimeMillis() - Integer.parseInt(x)*1000*60L), intervalMinutes))
  }

  private def innerLeq(x:String): Seq[Int] = x.toLowerCase match {
    case ""|"null"|"empty"|"none"|")nil" => Seq()
    case _ => {
      val now = System.currentTimeMillis()
      val t1 = roundDate(now - Integer.parseInt(x)*1000*60L, intervalMinutes)
      var t2 = Math.min(t1, roundDate((now - maxDurationMinutes*1000*60L), intervalMinutes))
      val b = mutable.Buffer[Int]()
      while (t2 < t1) {
        b += t2
        t2 += 1
      }
      (Seq[Int]() :+ 0) ++ b :+ t2
    }
  }



  private def innerGeq(x:String): Seq[Int] = x.toLowerCase match {
    case ""|"null"|"empty"|"none"|")nil" => Seq()
    case _ => {
      val now = System.currentTimeMillis()
      var t = roundDate((now - Integer.parseInt(x)*1000*60L), intervalMinutes)
      val t1 = roundDate(now, intervalMinutes)
      val b = mutable.Buffer[Int]()
      while (t < t1) {
        b += t
        t += 1
      }
      Seq[Int]() ++ b :+ t
    }
  }

  override protected def innerOperMap(param:String) = Map(":" -> innerEq, ">" -> innerGeq, "<" -> innerLeq)

  override protected def innerSerDe(param: String): (Int => Array[Byte], Array[Byte] => Int) =
   {x: Int => Snappy.compress(x.toString)} -> {v: Array[Byte] => Snappy.uncompressString(v).toInt}

}

class CdcAgentTimestampCacheQueryFunctionProvider[E<:CDC[E]](val provider: CDCGeneratedEnumsProvider, override val intervalMinutes: Int, override val maxDurationMinutes: Int)
  extends TimestampCacheQueryFunctionProvider[E]{

  override protected def innerFunction(param: String)(v: E) = {
    val e = provider.enums.find(_.name == param).get
    Seq(v.lastUpdatedBy(e.asInstanceOf[CDCAgent]).map(t => roundDate(t.getMillis, intervalMinutes)).getOrElse(0))
  }
}

/**
  * UDF name constants
  */
object CacheQueryConstants {

  val FIELD_VALUE = "FieldValue"

  val CDC_AGENT_TIMESTAMP = "CdcAgentTimestamp"

}

trait CacheShardedEntityOperation extends ShardedEntityOperation{

  override def getServiceName: String = "cache"

  override def getServiceHandler(serviceActor: ActorRef): ShardedEntityHandler = new CacheShardHandler(serviceActor)

}

trait CachePassivate extends PassivateShardedEntityOperation with CacheShardedEntityOperation

/**
  * Some cache operations require passing a small set of key-value pairs as a string: we use URL-encoding query string
  * and put it to the 'id' of the underlying sharded entity message
  */
trait EncodedKeyCacheShardEntityOperation extends CacheShardedEntityOperation {

  def encodeKey(args: (String,String)*): String = {
    val s = args.toList.foldLeft("")((x,s) => (Option(s._1), Option(s._2)) match {
      case (Some(k), Some(v)) => x + s"$k=$v&"
      case _ => x
    })
    if (!s.isEmpty) s.substring(0, s.size-1) else s
  }

  def decodeKey(query: String): Map[String,String] =
    if (!query.isEmpty){
      Map() ++ query.split("&").map(x => {
        val s = x.split("=")
        s(0) -> s(1)
      })} else Map()

  def header[E<:CDC[E]](shardId: String)(args: (String,String)*)(implicit cmp: GeneratedMessageCompanion[E]): ShardedEntity =
    cmp.defaultInstance.setKey(encodeKey(args:_*)).setShardID(shardId).setOperation(this)

}


/**
  * Trait that has meta-data and code for all indexes
  * @tparam E
  */
trait CachedEntityIndexer[E<:CDCEntity]{

  type IndexDefinition = (String, CacheQueryFunction[E], () => ReverseIndex[_])

  type PrimaryIndexDefinition = (FieldDescriptor, () => PrimaryIndex[E])

  val secondaryIndexes: Seq[IndexDefinition]

  val primaryIndex: PrimaryIndexDefinition

}

case class CacheConsumerGroupMetadata(initialOffset: DateTime, includeFilter: Option[Set[CDCAgent]], excludeFilter: Option[Set[CDCAgent]])

/**
  * Base trait for all cache indexes
  * See child traits
  */
trait CacheIndex{

  type V

  type K

  type Repr

  /**
    * Currently, uses a map - but can be replaced with skip-list or a merge-tree
    */
  val map: scala.collection.concurrent.TrieMap[K, Repr] = TrieMap[K,Repr]()

  def add[K1<:K,V1<:V](key: K1, values: V1*): Unit

  def remove[K1<:K,V1<:V](key: K1, values: V1*): Unit

}

/**
  * Primary index data structure: stores compressed BLOBs of the underlying entities keyed by natural keys
  * @param compactor
  * @tparam E
  */
case class PrimaryIndex[E<:CDCEntity](val compactor: CachedEntityCompactor[E, Array[Byte]]) extends CacheIndex{
  override type V = E
  override type K = String
  override type Repr = Array[Byte]

  override def add[K1<:K, V1<:V](key: K1, values: V1*) = {
    values.size match {
      case 0 => {}
      case _ => map.put(key, compactor.deflate(values.last))
    }
  }

  override def remove[K1 <: K, V1 <: V](key: K1, values: V1*): Unit = {
    values.size match {
      case 0 => {}
      case _ => map.remove(key) match {
        case Some(x) => if (!values.contains(compactor.inflate(x))) map.put(key,x)
        case _ => {}
      }
    }
  }
}

/**
  * Reverse index data structure: stores natural key lists (mutable) keyed by values (any type)
  * @tparam T
  */
case class ReverseIndex[T]() extends CacheIndex{

  override type V = String
  override type K = T
  override type Repr = mutable.LinkedHashSet[String]

  override def add[K1<:K,V1<:V](key: K1, values: V1*) = map.getOrElseUpdate(key, new mutable.LinkedHashSet[String]()) ++= values

  override def remove[K1 <: K, V1 <: V](key: K1, values: V1*): Unit = map.getOrElseUpdate(key, new mutable.LinkedHashSet[V]()) --= values
}

/**
  * Data holder class for anything related to a cache shard: indexes, consumer queues
  * @param shardId
  * @param cmp
  * @tparam E
  */
class CacheShard[E<:CDC[E]](val shardId: String, val cmp: CDCEntityCompanion[E] with CachedEntityIndexer[E]) extends LazyLogging {

  import CacheShardExtension._

  type UpdateData = (String, E, Option[E])

  type IndexSnapshotData = (String, scala.collection.concurrent.TrieMap[String, mutable.LinkedHashSet[Any]])

  val isIndexSnapshotted = cmp.isInstanceOf[SnapshottedCacheEntityCompanion[_]]

  val secondaryIndexesMap: scala.collection.concurrent.TrieMap[String, (CacheQueryFunction[E],ReverseIndex[_])] = {
    val m = TrieMap[String,(CacheQueryFunction[E],ReverseIndex[_])]()
      cmp.secondaryIndexes.foreach(d => m.putIfAbsent(d._1, d._2 -> d._3.apply()))
      m
  }

  // unflushed records is a temporary buffer to store info that needs to be persisted to Cass during flush
  val unflushedRecords =  scala.collection.concurrent.TrieMap[String, IndexSnapshotData]()

  val primaryIndexFieldDescriptor = cmp.primaryIndex._1

  val primaryIndex: PrimaryIndex[E] = cmp.primaryIndex._2.apply()

  val indexingInProcess: AtomicReference[Boolean] = new AtomicReference[Boolean](false)

  val groupQueuesMap: scala.collection.concurrent.TrieMap[String, scala.collection.mutable.LinkedHashSet[String]] = TrieMap[String,scala.collection.mutable.LinkedHashSet[String]]()

  val groupMetadataMap: scala.collection.concurrent.TrieMap[String, CacheConsumerGroupMetadata] = TrieMap[String,CacheConsumerGroupMetadata]()

  private var bypassFilters = true

  def getGroupQueue(group: String) = groupQueuesMap.getOrElseUpdate(group, new scala.collection.mutable.LinkedHashSet[String] with mutable.SynchronizedSet[String])

  def recoverGroupQueue(group: String): Unit =
    groupQueuesMap.get(group).foreach(r => groupMetadataMap.get(group).foreach(_ => {
      r.clear()
      primaryIndex.map.foreach(v => addIfRelevant(group, primaryIndex.compactor.inflate(v._2), v._1, None))}
    ))

  /**
    * Recovers all indexes based on the data retrieved from Cass
    * @param snapshots
    */
  def recoverIndexes(snapshots: Map[String, Seq[CacheIndexSnapshot]]) = {
    val nullBlobs = snapshots.toSeq.flatMap(e => e._2.filter(x => x.payload.isEmpty || x.payload.get.indexValues.isEmpty
      || x.payload.get.indexValues.find(_.isEmpty).isDefined).map(_.id)).toSet
    if (!nullBlobs.isEmpty) {
      logger.warn(s"Found entries with null blobs in cache shard $shardId: $nullBlobs")
      val default = cmp.defaultInstance
      val v = nullBlobs.toSeq.map(k => Try {
        val y = default.setKey(k) match {
          case v: AutoShardedEntity[E] => v.asInstanceOf[AutoShardedEntity[E]].processShardId(CdcProcessingUpdate)
          case z => z
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(y.getShardID))
        y
      }).filter(_.isSuccess).map(_.get.asInstanceOf[E])
      if (!v.isEmpty){
        val comp = v.head.setOperation(CdcProcessingUpdate).companion
        val resolver = cmp.asInstanceOf[CDCEntityResolver[_]]
        val defaultCdcUpdate: Option[resolver.DT] = Some(comp.defaultInstance.asInstanceOf[ShardedEntity]
          .setOperation(CdcProcessingUpdate).asInstanceOf[resolver.DT])
        v.groupBy(_.getShardID).values.toSeq.map(l =>
          resolver.wrapCDCEntities(l.map(_.asInstanceOf[resolver.CD]), defaultCdcUpdate).asInstanceOf[ShardedEntity]
        )
        .foreach(m => {
          AkkaCluster.messageShardingService(m)(null)
        })
      }
    }
    snapshots.foreach(e => e._1 match {
      case PRIMARY_CACHE_INDEX_ID => recoverPrimaryIndex(e._2.filterNot(x => nullBlobs.contains(x.id)))
      case _ => recoverSecondaryIndex(e._1, e._2.filterNot(x => nullBlobs.contains(x.id)))
    })
  }

  private def recoverPrimaryIndex(snapshots: Seq[CacheIndexSnapshot]) = {
    val m = primaryIndex.map
    m.clear()
    snapshots.map(e => e.id -> Try{e.payload.get.indexValues.head.get}).filter(_._2.isSuccess).foreach(e => m.putIfAbsent(e._1, e._2.get))
  }

  private def recoverSecondaryIndex(indexId: String, snapshots: Seq[CacheIndexSnapshot]) = {
    val index = secondaryIndexesMap(indexId)
    val queryFunction = index._1
    val m = index._2
    m.map.clear()
    val toAdd = snapshots.flatMap(v => v.payload.get.indexValues.filter(_.isDefined).map(x => v.id -> queryFunction.deserialize(x.get)))
    toAdd.groupBy(_._2).toSeq.map(w => w._1 -> w._2.map(_._1)).foreach(e => m.add(e._1.asInstanceOf[m.K], e._2:_*))
  }

  def updateBypassFiltersFlag: Unit = {
    bypassFilters = groupMetadataMap.foldLeft(true)((b, x) => b && x._2.includeFilter.isEmpty && x._2.excludeFilter.isEmpty)
  }

  private def latest(a: DateTime, b: DateTime) = if (a.isAfter(b)) a else b

  // see time and agent filtering logic in this method
  private def addIfRelevant(group: String, element: E, deflated: String, timestamp: Option[DateTime]): Unit =
    groupQueuesMap.get(group).foreach(r => groupMetadataMap.get(group).foreach(m => {
      val relevantUpdaters = element.updatesSince(latest(timestamp.getOrElse(m.initialOffset), m.initialOffset)).keySet
      if (!relevantUpdaters.isEmpty){
      val shouldInclude = m.includeFilter match {
        case Some(f) => !f.intersect(relevantUpdaters).isEmpty
        case _ => true
      }
      val shouldExclude = m.excludeFilter match {
        case Some(f) => !f.intersect(relevantUpdaters).isEmpty
        case _ => false
      }
      if (shouldInclude && !shouldExclude) r += deflated}}
    ))
  // fast intersect methods for huge sets
  def intersect0[T1,T2](x: scala.collection.Set[T1], y: scala.collection.Set[T2]): scala.collection.Set[Any] = if (x.size <= y.size) (x.map(_.asInstanceOf[T2]) & y).map(_.asInstanceOf[Any]) else (y.map(_.asInstanceOf[T1]) & x).map(_.asInstanceOf[Any])

  def intersect1[T](x: scala.collection.Set[T], y: scala.collection.Set[T]): scala.collection.Set[T] = if (x.size <= y.size) (x & y) else (y & x)

  // method to retrieve queried data from a secondary index
  def query(secondaryIndexKey: String, queryOperatorKey: String, queryOperatorArg: String): scala.collection.Set[String] = {
    val i = secondaryIndexesMap(secondaryIndexKey)
    val r = intersect0(i._1.supportedOperators(queryOperatorKey).generateValueRange(queryOperatorArg).toSet, i._2.map.keySet)
    r.flatMap(w => i._2.map.get(w.asInstanceOf[i._2.K]).getOrElse(Set[String]()))
  }

  /**
    * Cache mutation method (maintains indexes)
    * @param values
    */
  def put(values: Seq[E]): Unit = {
    val valuesSeq: Seq[UpdateData] = values.map(v => {
      val k = extractPrimaryIndexValue(v)
      (k, v, primaryIndex.map.get(k).map(primaryIndex.compactor.inflate(_)))
    })
    updateIndexes(valuesSeq)
    if (bypassFilters) {
      //bulk insert
       groupQueuesMap.foreach(r => groupMetadataMap.get(r._1).foreach(_ => r._2 ++= valuesSeq.map(_._1)))
    } else valuesSeq.foreach(v => {
        // insert one-by-one
        val lastUpdatedOption: Option[DateTime] = v._3.map(_.lastUpdated)
        groupMetadataMap.keys.toSet[String].foreach(g => addIfRelevant(g, v._2, v._1, lastUpdatedOption))
    })
  }

  private def updateIndexes(values: Seq[UpdateData]) = {
    val tick = System.currentTimeMillis()
    indexingInProcess.set(true)
    updatePrimaryIndex(values)
    secondaryIndexesMap.foreach(e => updateSecondaryIndex((e._2._1,e._2._2,e._1),values))
    logger.debug(s"Indexed ${values.size} entities in cache shard $shardId, latency: ${System.currentTimeMillis() - tick}")
    indexingInProcess.set(false)
  }

  /**
    * Flush method: saves index snapshots to Cass and signals CDC Processing status changes back to CDC shards
    * Note: this is a thread-safe, highly-concurrent, non-blocking method. Lots of thinking went into it
    * @return
    */
  def flush: Future[Unit]  = {
    implicit val ec = CacheShardExtension.cacheEc
    cmp match {
      case c: SnapshottedCacheEntityCompanion[CDC[E]] => {
        val copy = unflushedRecords.take(CACHE_FLUSH_MAX_PACKET_SIZE).toSeq
        val toFlush = copy.flatMap(x => x._2._2.map(e => (shardId, e._1, x._1, e._2)))
        val toSend = copy.map(x => x._1 -> x._2._1).toSet
        if (!toFlush.isEmpty){
          val default = cmp.asInstanceOf[GeneratedMessageCompanion[_]].defaultInstance.asInstanceOf[E]
          val comp = default.setShardID(toSend.head._2).setOperation(CdcProcessingUpdate).companion
          val resolver = comp.asInstanceOf[CDCEntityResolver[_]]
          val wrapper: Option[resolver.DT] = Some(comp.defaultInstance.asInstanceOf[ShardedEntity].setOperation(CdcProcessingUpdate).asInstanceOf[resolver.DT])
          val groupedMessages = toSend.groupBy(x => x._2).mapValues(l => {
            resolver.wrapCDCEntities(l.toSeq.map(s => default.setKey(s._1).setShardID(s._2).asInstanceOf[resolver.CD]), wrapper)
          })
          val groupedPrimaryIndexSnapshots: Seq[Seq[CacheIndexSnapshot]] = toFlush.groupBy(_._1).toSeq.map(e => e._2.map(_._3).toSet
            .map[CacheIndexSnapshot, Set[CacheIndexSnapshot]](id => CacheIndexSnapshot(e._1, PRIMARY_CACHE_INDEX_ID, id, Some(CacheIndexPayload(Seq(primaryIndex.map.get(id)))))).toSeq)
          val groupedSecondaryIndexSnapshots: Seq[Seq[CacheIndexSnapshot]] = toFlush.groupBy(x => x._1 -> x._2).toSeq.map(v => {
             val queryFunc = secondaryIndexesMap(v._1._2)._1
             v._2.map(l => CacheIndexSnapshot(l._1, l._2, l._3, Some(CacheIndexPayload(l._4.toSeq.map(o => Some(queryFunc.serialize(o.asInstanceOf[queryFunc.R])))))))
          })
          val tick = System.currentTimeMillis()
          val s = (groupedPrimaryIndexSnapshots ++ groupedSecondaryIndexSnapshots).map(c.upsertToSnapshot(_))
          Future.sequence(s).map[Unit](_ => {
            groupedMessages.foreach(x => AkkaCluster.messageShardingService(x._2.asInstanceOf[ShardedEntity])(null))
            logger.info(s"Flushed ${toSend.size} cached entities to index snapshot in shard $shardId, latency: ${System.currentTimeMillis() - tick}")
            unflushedRecords --= toSend.map(_._1).toSet
          })
        } else Future.successful()
      }
      case _ => Future.successful()
    }
  }

  def extractPrimaryIndexValue(e:E): String = e.getField(primaryIndexFieldDescriptor).asInstanceOf[PString].value

  /**
    * Get by key (in bulk) from primary index
    * @param keys
    * @param tryNum
    * @param pause
    * @return
    */
  def collect(keys: Iterable[String], tryNum: Int = 1, pause: FiniteDuration = CACHE_COLLECT_RETRY_PAUSE): Seq[E] = {
    val r = keys.map(k => primaryIndex.map.get(k))
      .filter(_.isDefined).map(x => primaryIndex.compactor.inflate(x.get))
    if ((r.size < keys.size) && indexingInProcess.get && tryNum < CACHE_COLLECT_RETRY_MAX){
      logger.info(s"Cache ${shardId} collect has collided with indexing in progress, retry #$tryNum in ${pause.toMillis} millis")
      Thread.sleep(pause.toMillis)
      collect(keys, tryNum + 1, pause * 2)
    } else r.toSeq
  }

  private def updatePrimaryIndex(values: Seq[UpdateData]) = {
    val v = primaryIndexFieldDescriptor -> primaryIndex
    val valuesToAdd: Map[v._2.K, Seq[E]] = values.map(x => x._1 -> x._2)
      .groupBy(_._1)
      .map(e => e._1.asInstanceOf[v._2.K] -> e._2.map(_._2))
    val valuesToRemove: Map[v._2.K, Seq[E]] = values.filter(_._3.isDefined).map(x => x._1 -> x._3.get)
      .groupBy(_._1)
      .map(e => e._1.asInstanceOf[v._2.K] -> e._2.map(_._2))
    valuesToRemove.foreach(z => {
      v._2.remove(z._1, z._2:_*)
    })
    valuesToAdd.foreach(z => {
      v._2.add(z._1, z._2:_*)
    })
    if (isIndexSnapshotted) {
      val u = values.map(e => e._1 -> (e._2 match {
        case v: AutoShardedEntity[E] => v.asInstanceOf[AutoShardedEntity[E]].processShardId(CdcProcessingUpdate)
        case z => z
      }).getShardID)
      u.foreach(x => unflushedRecords.getOrElseUpdate(x._1, x._2 -> scala.collection.concurrent.TrieMap()))
    }
  }

  private def updateSecondaryIndex(v: (CacheQueryFunction[E],ReverseIndex[_], String), values: Seq[UpdateData]) = {
    val valuesToAdd: Map[v._2.K, Seq[String]] = values.flatMap(x => v._1.apply(x._2).map(_ -> x._1))
      .groupBy(_._1)
      .map(e => e._1.asInstanceOf[v._2.K] -> e._2.map(_._2))
    val valuesToRemove: Map[v._2.K, Seq[String]] = values.filter(_._3.isDefined).flatMap(x => v._1.apply(x._3.get).map(_ -> x._1))
      .groupBy(_._1)
      .map(e => e._1.asInstanceOf[v._2.K] -> e._2.map(_._2))
    valuesToRemove.foreach(z => {
      v._2.remove(z._1, z._2:_*)
    })
    valuesToAdd.foreach(z => {
      v._2.add(z._1, z._2: _*)
    })
    val empty = mutable.LinkedHashSet[String]()
    v._2.map.filter(_._2.isEmpty).foreach(x => v._2.map.remove(x._1,empty))
    if (isIndexSnapshotted) {
      val groupedValuesToRemove = valuesToRemove.toSeq.flatMap(e => e._2.map(_ -> e._1)).groupBy(_._1)
      val groupedValuesToAdd = valuesToAdd.toSeq.flatMap(e => e._2.map(_ -> e._1)).groupBy(_._1)
      values.foreach(x => {
        val r = unflushedRecords.getOrElseUpdate(x._1, (x._2 match {
          case m: AutoShardedEntity[E] => m.asInstanceOf[AutoShardedEntity[E]].processShardId(CdcProcessingUpdate)
          case z => z
        }).getShardID -> scala.collection.concurrent.TrieMap())._2.getOrElseUpdate(v._3, mutable.LinkedHashSet())
        val toRemove = groupedValuesToRemove.get(x._1).getOrElse(Seq()).map(_._2.asInstanceOf[Any]).toSet
        val toAdd = groupedValuesToAdd.get(x._1).getOrElse(Seq()).map(_._2.asInstanceOf[Any]).toSet
        r --= toRemove
        r ++= toAdd
      })
    }
  }

}

object CacheRetry {

  val flushTypeToken = new TypeToken[CacheRetry[_]](){}

  val rnd = new Random()
}

/**
  * Trait that triggers Retry logic (periodic cache query to identify stale entries to be touched to trigger re-processing)
  * @tparam E
  */
trait CacheRetryProvider[E<:CDCEntity]{

  def apply(shardId: String, isRunning: AtomicBoolean, parentRef: ActorRef): CacheRetry[E]

}

/**
  * Retry action
  * Note: it rate-limits the retry to slowly drain the retry backlog
  * See the child implementation
  * @tparam E
  */
trait CacheRetry[E<:CDCEntity] extends LazyLogging with ShardedEntityActorDispatchAction {


   protected val isRunning: AtomicBoolean
   protected val query: ShardedEntity
   protected val parentRef: ActorRef
   val period: Int

   override type T = CacheRetry[_]

   override val typeToken =  CacheRetry.flushTypeToken

   protected def retry(retryList: Seq[E]): Unit

  override def execute(prev: Option[Try[T]]) = {
    import scala.concurrent.duration._
    implicit val ec = CacheShardExtension.cacheEc
    if (isRunning.get()){
      val f = AkkaCluster.asyncCallShardingService(query, Some(period.seconds))
      f.onSuccess{case m: ShardedEntity => {
        val entities = m.companion.asInstanceOf[CDCEntityResolver[m.type]].resolveCDCEntities(m).map(_.asInstanceOf[E]).take(CacheShardExtension.CACHE_RETRY_MAX_SET_SIZE)
        if (!entities.isEmpty) {
          logger.info(s"Retrying ${entities.size} entities in cache shard ${query.getShardID}: ${entities.map(_.getKey)}")
          retry(entities)
        }
      }}
      AkkaCluster.actorSystem.scheduler.scheduleOnce(period.seconds)(parentRef.tell(this,null))
      Future.successful[T](this)
    } else Future.failed[T](new IllegalStateException(s"Could not retry because the cache shard actor has stopped for shard ${query.getShardID}"))
  }
}


object CacheFlush {

  val flushTypeToken = new TypeToken[CacheFlush[_]](){}

  val rnd = new Random()
}

/**
  * Cash flush to persist index updates to Cass
  * @param isRunning
  * @param cacheShard
  * @param parentRef
  * @param period
  * @tparam E
  */
case class CacheFlush[E<:CDC[E]](val isRunning: AtomicBoolean,
                      val cacheShard: CacheShard[E],
                      val parentRef: ActorRef,
                      val period: Int) extends LazyLogging with ShardedEntityActorDispatchAction{

  override type T = CacheFlush[_]

  override val typeToken =  CacheFlush.flushTypeToken

  override def execute(prev: Option[Try[T]]) = {
    import scala.concurrent.duration._
    val shardId = cacheShard.shardId
    implicit val ec = CacheShardExtension.cacheEc
    if (isRunning.get()){
      val f = cacheShard.flush.recover{case x: Throwable => logger.error(s"Caught exception while running cache flush task for shard $shardId: $x")}
      f.map(_ => {
        val flush = CacheFlush(this.isRunning,this.cacheShard, this.parentRef,this.period)
        AkkaCluster.actorSystem.scheduler.scheduleOnce(period.seconds)(parentRef.tell(flush,null))
        flush
      })
    } else Future.failed[T](new IllegalStateException(s"Could not flush because the cache shard actor has stopped for shard ${shardId}"))
  }
}

/**
  * Cache service sharding handler
  * @param parentRef
  */
class CacheShardHandler(override protected val parentRef:ActorRef) extends BaseShardedEntityHandler with LazyLogging{

  import CacheShardRegistry._

  import collection.JavaConverters._

  parentRef ! ShardedEntityActorOnStop(() => this.terminate())

  private lazy val agentEnums = CDCShardExtension.get(AkkaCluster.actorSystem).cassContext.enums

  private val cachesMap = new ConcurrentHashMap[String, CacheShard[_<:CDCEntity]]()

  val shardInitializers = TrieMap[String, CacheShardInitializer[_<:CDCEntity]]()

  val isRunning = new AtomicBoolean(true)

  class CacheShardInitializer[E<:CDC[E]](cmp: CDCEntityCompanion[E] with CachedEntityIndexer[E]) extends java.util.function.Function[String, CacheShard[E]]{

    /**
      * This method handles cache recovery from Cass
      * Two cases: huge (recovers from index snapshots) or small (recovers from CDC entity table and recalculates all indexes)
      * @param shardID
      * @return
      */
    override def apply(shardID: String): CacheShard[E] = {
      implicit val sender = parentRef
      logger.info(s"Created cache shard ${shardID}")
      val result = new CacheShard[E](shardID, cmp)
      Try{
        val tick = System.currentTimeMillis()
        import scala.concurrent.duration._
        logger.debug(s"Running cache recovery for shard ${shardID}")
        cmp match {
          case comp: SnapshottedCacheEntityCompanion[E] => {
            implicit val ec = CacheShardExtension.cacheEc
            val m: Map[String, Seq[CacheIndexSnapshot]] = Await.result(comp.queryFromSnapshot(shardID, PRIMARY_CACHE_INDEX_ID).flatMap(l1 => {
                  val s = result.secondaryIndexesMap.toSeq.map(e => comp.queryFromSnapshot(shardID,e._1).map(l => e._1 -> l))
                  Future.sequence(s).map(l2 => (Map[String, Seq[CacheIndexSnapshot]]() + (PRIMARY_CACHE_INDEX_ID -> l1) ++ l2))
                }), 300.seconds)
            result.recoverIndexes(m)
            logger.info(s"Recovered ${m(PRIMARY_CACHE_INDEX_ID).size} primary index entries and ${m.size -1} secondary index snapshots in shard ${shardID}, latency: ${System.currentTimeMillis() - tick}")
          }
          case _ => {
            val entities = Await.result(cmp.queryEntities(shardID), 300.seconds).map(_ match{
              case v: AutoShardedEntity[E] => Try{v.processShardId(CacheUpdate)}
              case x => Success(x)
            }).filter(_.isSuccess).map(_.get)
            logger.debug(s"Retrieved ${entities.size} entities from Cassandra DB in shard ${shardID}, latency: ${System.currentTimeMillis() - tick}")
            innerHandleUpdate[E](entities, result)
            logger.info(s"Recovered ${entities.size} cached entities in shard ${shardID}, latency: ${System.currentTimeMillis() - tick}")
          }
        }
      }.map[Unit](_ => CacheShardExtension.get(AkkaCluster.actorSystem).messageShardRegistry(cacheShardRegistration[E](
        CACHE_STATUS_REGISTERING, shardID, getReplicaId)(ClassTag[E](cmp.clazz))))
      match {
        case Failure(x) => onCacheShardStartFailure(shardID,x)
        case _ => {
          if (result.isIndexSnapshotted) {
            implicit val ec = AkkaCluster.miscEc
            import scala.concurrent.duration._
            val period = CacheShardExtension.config(cmp.clazz.getName).flushPeriod
            val flush = CacheFlush[E](isRunning, result, parentRef, period)
            val half = Math.ceil(period/2).toInt
            AkkaCluster.actorSystem.scheduler.scheduleOnce((period + CacheFlush.rnd.nextInt(period) - half).seconds)(parentRef.tell(flush, null))
          }
          Try{AkkaCluster.injector.getInstance(Key.get(classOf[CacheRetryProvider[E]],
            Names.named(s"${cmp.clazz.getSimpleName}")))} match {
               case Success(p: CacheRetryProvider[E]) => {
                 val retry = p.apply(shardID, isRunning, parentRef)
                 implicit val ec = AkkaCluster.miscEc
                 import scala.concurrent.duration._
                 val retryPeriod = retry.period
                 val half = Math.ceil(retryPeriod/2).toInt
                 AkkaCluster.actorSystem.scheduler.scheduleOnce((retryPeriod + CacheRetry.rnd.nextInt(retryPeriod) - half).seconds)(parentRef.tell(retry, null))
               }
               case _ => {}
          }
        }
      }
      result
    }
  }

  private def resolveCDCEntities[E<:ShardedEntity](e: E) = e.companion.asInstanceOf[CDCEntityResolver[E]].resolveCDCEntities(e)

  private def wrapCDCEntities[E<:ShardedEntity](e: E, values: Seq[CDCEntity]): E = {
    val resolver = e.companion.asInstanceOf[CDCEntityResolver[E]]
    val cmp = e.companion
    val defaultVal = e match {
      case u: AsyncAskMessage[_] => cmp.defaultInstance.asInstanceOf[AsyncAskMessage[_]].withCorrelationId(u.correlationId)
      case _ => cmp.defaultInstance
    }
    resolver.wrapCDCEntities(values.map(_.asInstanceOf[resolver.CD]),
      Some(defaultVal.asInstanceOf[resolver.DT].setOperation(e.getOperation).asInstanceOf[resolver.DT])).asInstanceOf[E]
  }

  private def getCachedEntityCompanion[E<:CDCEntity](e: E): CDCEntityCompanion[e.CDE] with CachedEntityIndexer[e.CDE] = e.companion.asInstanceOf[CDCEntityCompanion[e.CDE] with CachedEntityIndexer[e.CDE]]

  private def getReplicaId: Int = {
    val path = parentRef.path.toStringWithoutAddress
    path.split("/").find(_.contains(CacheShardExtension.CACHE_REPLICA_SERVICE)) match {
      case Some(s) => {
        val r = (s.substring(s.lastIndexOf("-")+1).toInt + 1)
        r
      }
      case _ => 0
    }
  }

  private def onCacheShardStartFailure(shard: String, exc: Throwable) = {
    logger.error(s"Could not recover and register cache shard ${shard}, stopping it: $exc")
    parentRef ! PoisonPill
  }

  private def getOrUpdateCacheShard[E<:CDC[E]](shardID: String, cmp: CDCEntityCompanion[E] with CachedEntityIndexer[E]): CacheShard[E] = {
    val initializer = shardInitializers.getOrElseUpdate(shardID, new CacheShardInitializer[E](cmp))
    cachesMap.computeIfAbsent(shardID, initializer).asInstanceOf[CacheShard[E]]
  }

   override def innerHandle(message: ShardedEntity) = {
      val entities = resolveCDCEntities(message)
     entities.headOption match {
       case Some(head) => {
         val companion = getCachedEntityCompanion(head)
         val s = entities.map(_.asInstanceOf[head.CDE])
         Future{innerHandle0(message, s, message.getOperation,
           getOrUpdateCacheShard(message.getShardID, companion)).get}(CacheShardExtension.cacheEc)
       }
       case _ => Future.successful(None)
     }

   }

  private def decodeSet(string: String): Set[String] = string.split(",").toSet

  private def header(shardId: String, time: Long, quantity: Int)(cmp: AnyC) = CacheGroupPoll.header(shardId)(QUANTITY_KEY -> quantity.toString, TIMESTAMP_KEY -> time.toString)(cmp)

  private def innerHandleUpdate[E<:CDC[E]](entities: Seq[E], cache: CacheShard[E]) = Try{
    cache.put(entities)
    None
  }

  private def getCurrentGroupOffset(cache: CacheShard[_<:CDCEntity], group: String): Long = cache.getGroupQueue(group).headOption match {
    case Some(y) => Try{cache.collect(Seq(y)).head.lastUpdated.getMillis - 1}.getOrElse(System.currentTimeMillis())
    case _ =>  System.currentTimeMillis()
  }

  private def innerHandle0[E<:CDC[E]](message: ShardedEntity, entities: Seq[E], operation: ShardedEntityOperation, cache: CacheShard[E]): Try[Option[ClusterMessage]] = {
      operation match {

        case m: CacheGroupInitialize if (entities.size == 1) => Try{
          val head = entities.head
          val decodedKey = CacheGroupStart.decodeKey(head.getKey)
          val group = decodedKey(GROUP_ID_KEY)
          val timestamp =  new DateTime(decodedKey(TIMESTAMP_KEY).toLong, head.lastUpdated.getZone)
          val includeFilter: Option[Set[CDCAgent]] = Option(decodedKey.getOrElse(INCLUDE_FILTER_KEY, null)).map(decodeSet(_).map(agentEnums(_).asInstanceOf[CDCAgent]))
          val excludeFilter: Option[Set[CDCAgent]] =  Option[String](decodedKey.getOrElse(EXCLUDE_FILTER_KEY, null)).map(decodeSet(_).map(agentEnums(_).asInstanceOf[CDCAgent]))
          cache.groupMetadataMap.put(group,
            CacheConsumerGroupMetadata(timestamp, includeFilter, excludeFilter))
          cache.updateBypassFiltersFlag
          cache.recoverGroupQueue(group)
          None
        }

        case m: CacheGroupStart  if (entities.size == 1) => Try{
          val head = entities.head
          val decodedKey = m.decodeKey(head.getKey)
          val group = decodedKey(GROUP_ID_KEY)
          val timestamp =  new DateTime(decodedKey.getOrElse(TIMESTAMP_KEY, System.currentTimeMillis().toString).toLong, head.lastUpdated.getZone)
          val includeFilter: Option[Set[CDCAgent]] =  Option(decodedKey.getOrElse(INCLUDE_FILTER_KEY, null)).map(decodeSet(_).map(agentEnums(_).asInstanceOf[CDCAgent]))
          val excludeFilter: Option[Set[CDCAgent]] =  Option[String](decodedKey.getOrElse(EXCLUDE_FILTER_KEY, null)).map(decodeSet(_).map(agentEnums(_).asInstanceOf[CDCAgent]))
          val q = cache.getGroupQueue(group)
          cache.groupMetadataMap.putIfAbsent(group,
            CacheConsumerGroupMetadata(timestamp, includeFilter, excludeFilter))
          cache.updateBypassFiltersFlag
          cache.recoverGroupQueue(group)
          val time = getCurrentGroupOffset(cache, group)
          logger.debug(s"Shard ${cache.shardId} has started a message queue for consumer group $group: backlogged ${q.size} messages")
          Some(header(cache.shardId, time, q.size)(head.companion.asInstanceOf[AnyC]))
        }

        case m: CacheGroupStop  if (entities.size == 1) => Try{
          val head = entities.head
          val decodedKey = CacheGroupStart.decodeKey(head.getKey)
          val group = decodedKey(GROUP_ID_KEY)
          cache.groupQueuesMap.remove(group)
          cache.groupMetadataMap.remove(group)
          cache.updateBypassFiltersFlag
          logger.debug(s"Shard ${cache.shardId} has removed a message queue for consumer group $group")
          None
        }

        // poll: dequeues from consumer queue and provides back the remaining queue size
        case m: CacheGroupPoll  if (entities.size == 1) => Try{
          val head = entities.head
          val decodedKey = CacheGroupPoll.decodeKey(head.getKey)
          val group = decodedKey(GROUP_ID_KEY)
          val quantity = decodedKey(QUANTITY_KEY).toInt
          val q = cache.getGroupQueue(group)
          val r = q.take(Math.min(quantity, q.size)).toSeq
          q --= r
          val results = cache.collect(r)
          val time = getCurrentGroupOffset(cache, group)
          logger.debug(s"Cache shard ${cache.shardId} is returning ${results.size} polled entities (backlog = ${q.size}) to the consumer group $group")
          Some(CacheGroupPoll.response[E](header(cache.shardId,time,q.size)((head.companion.asInstanceOf[AnyC])),
            results))
        }

        case m: CacheUpdate => innerHandleUpdate(entities, cache)

        case m: CacheGet => {
          val keys = entities.map(v => cache.extractPrimaryIndexValue(v))
          val r = cache.collect(keys)
          if (r.size == entities.size) Success(Some(wrapCDCEntities(message, r))) else
            Failure(new IllegalStateException(s"Failed to retrieve messages by keys ${keys}"))
        }

          // cache query: supports both reverse-index lookups (based on set intersections) and full cache scans (slow but does not need an index)
        case m: CacheQuery if (entities.size == 1) => Try{
          val head = entities.head
          val cmp = head.cdcCompanion
          val prefix = cache.cmp.clazz.getSimpleName
          val injector = AkkaCluster.injector
          val tick = System.currentTimeMillis()
          val decodedQuery = m.decodeKey(head.getKey).toSeq
          val andPredicates: Set[(Either[String, CacheQueryFunction[E]], String, String)] = decodedQuery.filter(_._1.contains(AND_PREDICATE)).map(y => {
             val q: (String, CacheQueryFunction[E], Boolean) = cache.secondaryIndexesMap.find(e => {
               val split = e._1.split(":")
               y._2 == split(0) && y._1.contains(split(1))
              }) match {
                case Some(x) => (x._1,x._2._1,true)
                case _ => {
                  val f = injector.getInstance(Key.get(classOf[CacheQueryFunctionsProvider[E]],Names.named(s"$prefix-${y._2}")))
                  val o = f.supportedOperatorNames.find(y._1.contains(_)).get
                  val k = y._1.split(AND_PREDICATE)(1).split(o)(0)
                  logger.warn(s"Cache query [shard = ${cache.shardId}, type = ${y._2}, param = $k] is slow without supporting secondary index")
                  (s"${y._2}:$k",f.apply(k),false)
                }
              }
              val o = q._2.supportedOperators.keySet.find(y._1.contains(_)).get
              val a = y._1.split(o)(1)
              if (q._3) (Left(q._1), o, a) else (Right(q._2), o, a)
            }).toSet
           val orPredicates: Set[(Either[String, CacheQueryFunction[E]], String, String)] = decodedQuery.filter(_._1.contains(OR_PREDICATE)).map(y => {
             val q: (String, CacheQueryFunction[E], Boolean) = cache.secondaryIndexesMap.find(e => {
               val split = e._1.split(":")
               y._2 == split(0) && y._1.contains(split(1))
             }) match {
               case Some(x) => (x._1,x._2._1,true)
               case _ => {
                 val f = injector.getInstance(Key.get(classOf[CacheQueryFunctionsProvider[E]],Names.named(s"$prefix-${y._2}")))
                 val o = f.supportedOperatorNames.find(y._1.contains(_)).get
                 val k = y._1.split(OR_PREDICATE)(1).split(o)(0)
                 logger.warn(s"Cache query [shard = ${cache.shardId}, type = ${y._2}, param = $k] is slow without supporting secondary index")
                 (s"${y._2}:$k",f.apply(k),false)
               }
             }
             val o = q._2.supportedOperators.keySet.find(y._1.contains(_)).get
             val a = y._1.split(o)(1)
             if (q._3) (Left(q._1), o, a) else (Right(q._2), o, a)
          }).toSet

          def get(source:  Option[scala.collection.Set[String]], arg: (Either[String, CacheQueryFunction[E]], String, String)): scala.collection.Set[String] = arg._1 match {
            case Left(s) => {
              val z = cache.query(s, arg._2, arg._3)
              source match {
                case Some(w) => cache.intersect1(w,z)
                case _ => z
              }
            }
            case Right(f) => {
              val r = f.supportedOperators(arg._2).generateValueRange(arg._3).toSet
              source.map(s => cache.collect(s)).getOrElse(cache.collect(cache.primaryIndex.map.keySet))
                .filter(x => !cache.intersect0(f.apply(x).toSet,r).isEmpty).map(_.getKey).toSet
            }
          }

          val entityDigestFields = decodedQuery.filter(_._1.contains(DIGEST_FIELD)).map(y => y._2).toSet
          val r1 = andPredicates.foldLeft[Option[scala.collection.Set[String]]](None)((x,p) => Some(get(x,p)))
          val r2 = if (orPredicates.isEmpty) r1.getOrElse(cache.primaryIndex.map.keySet) else orPredicates.foldLeft[Set[String]](Set())((x,p) => x union get(r1,p))
          val r = cache.collect(r2)
          val tuple:(Int, Seq[E]) = entityDigestFields.isEmpty match {
            case true => (CacheShardExtension.CACHE_QUERY_MAX_RESULT_SET_SIZE, r)
            case false => (CacheShardExtension.CACHE_DIGEST_QUERY_MAX_RESULT_SET_SIZE, r.map(x => {
              cmp.messageReads.read(PMessage(x.toPMessage.value.filter(x => entityDigestFields.contains(x._1.name)))).setKey(x.getKey).setShardID(x.getShardID).asInstanceOf[E]
            }))
          }
          logger.debug(s"Cache query shard ${cache.shardId}: executed query request and finalized result set, total latency: ${System.currentTimeMillis() - tick}")
          if (tuple._2.size > tuple._1){
            logger.warn(s"Result set of the cache scan ${m} has size ${tuple._2.size}: trimming it to the configured threshold of ${tuple._1}")
          }
          Some(wrapCDCEntities(message, tuple._2.take(tuple._1)))
       }

        case _ => Failure(new UnsupportedOperationException(s"Operation ${operation} is not supported"))
      }
   }


  private def terminate(): Unit = {
    implicit val sender = parentRef
    isRunning.set(false)
    cachesMap.entrySet().asScala.foreach(v => {
      val r = v.getValue
      Try{Await.result(r.flush, 100.seconds)}
      r.primaryIndex.map.clear()
      r.secondaryIndexesMap.foreach(_._2._2.map.clear())
      r.groupQueuesMap.foreach(_._2.clear())
      CacheShardExtension.get(AkkaCluster.actorSystem).messageShardRegistry(cacheShardRegistration(
        CACHE_STATUS_UNREGISTERING, v.getKey, getReplicaId)(ClassTag[r.primaryIndex.V](r.cmp.clazz)
      ))
    })
    cachesMap.clear()
  }


}
