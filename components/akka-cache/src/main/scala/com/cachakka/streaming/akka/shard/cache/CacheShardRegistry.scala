package com.cachakka.streaming.akka.shard.cache

import java.lang.reflect.ParameterizedType
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import java.util.function.{BiFunction, BinaryOperator}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Props}
import akka.cluster.Cluster
import com.google.common.reflect.TypeToken
import com.trueaccord.scalapb.GeneratedEnum
import com.typesafe.scalalogging.Logger
import com.cachakka.streaming.akka.shard.cdc.CDCEntity
import com.cachakka.streaming.akka.{AkkaCluster, InstrumentalMessage, InstrumentalMessageCompanion, InstrumentalMessageType, _}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scalapb.descriptors.{PEnum, PMessage}


trait CacheRegistryStatus extends InstrumentalMessageType{}

trait CacheStatusRegistering extends CacheRegistryStatus {}

trait CacheStatusSubscribed extends CacheRegistryStatus {}

trait CacheStatusUnregistering extends CacheRegistryStatus {}

trait CacheStatusUnsubscribed extends CacheRegistryStatus {}


trait CacheRegistryMessage extends InstrumentalMessage{

  val status: CacheRegistryStatus

  def flipStatus(newStatus: CacheRegistryStatus): CacheRegistryMessage = {
    val cmp = companion
    val f = cmp.scalaDescriptor.findFieldByName("status").get
    cmp.messageReads.read(PMessage(this.toPMessage.value +
      (f->PEnum(companion.enumCompanionForFieldNumber(f.number).fromName(newStatus.toString).get.asInstanceOf[GeneratedEnum].scalaValueDescriptor)))).asInstanceOf[CacheRegistryMessage]
  }
}

trait CacheConsumerRegistrationTrait extends CacheRegistryMessage {
  override def getMessageType: CacheRegistryStatus = status

  val actorPath: String
  val entityFqn: String
  val groupId: String
  val hostname: String
  val parallelism: Int
  val index: Int


  lazy val entityClazz: Class[_<:CDCEntity] = Class.forName(entityFqn, true, getClass.getClassLoader).asInstanceOf[Class[_<:CDCEntity]]
  lazy val consumerActorSelection = AkkaCluster.actorSystem.actorSelection(actorPath)

  override def flipStatus(newStatus: CacheRegistryStatus): CacheConsumerRegistrationTrait = super.flipStatus(newStatus).asInstanceOf[CacheConsumerRegistrationTrait]
}

trait CacheConsumerRegistrationCompanionTrait extends InstrumentalMessageCompanion{

  override type M = CacheConsumerRegistration

}

trait CacheShardRegistrationTrait extends CacheRegistryMessage {
  override def getMessageType: CacheRegistryStatus = status

  val entityFqn: String
  val shardId: String
  val hostname: String
  val replica: Int

  lazy val entityClazz: Class[_<:CDCEntity] = Class.forName(entityFqn, true, getClass.getClassLoader).asInstanceOf[Class[_<:CDCEntity]]

  override def flipStatus(newStatus: CacheRegistryStatus): CacheShardRegistrationTrait = super.flipStatus(newStatus).asInstanceOf[CacheShardRegistrationTrait]

}

trait CacheShardRegistrationCompanionTrait extends InstrumentalMessageCompanion{

  override type M = CacheShardRegistration

}

/**
  * Monitoring API payload trait used by Swagger APIs to check the status of sharding
  */
trait CacheRegistryMonitoringOperation extends InstrumentalMessageType {}

trait GetCacheShardInfo extends CacheRegistryMonitoringOperation {

  def request[E<:CDCEntity](implicit ct:ClassTag[E]) = CacheShardInfo.defaultInstance.withEntityFqn(ct.runtimeClass.getName)

  def response[E<:CDCEntity](shards: Set[CacheShardRegistrationTrait])(implicit ct:ClassTag[E]) =
    CacheShardInfo.defaultInstance.withEntityFqn(ct.runtimeClass.getName).withShardMap(
      shards.groupBy(_.hostname).mapValues(s => CacheShards.defaultInstance.withShards(s.toSeq.map(_.asInstanceOf[CacheShardRegistration])))
    )

}

case object GetCacheShardInfo extends GetCacheShardInfo

trait GetCacheConsumerInfo extends CacheRegistryMonitoringOperation {

  def request[E<:CDCEntity](implicit ct:ClassTag[E]) = CacheConsumerInfo.defaultInstance.withEntityFqn(ct.runtimeClass.getName)

  def response[E<:CDCEntity](consumers: Set[CacheConsumerRegistrationTrait])(implicit ct:ClassTag[E]) =
    CacheConsumerInfo.defaultInstance.withEntityFqn(ct.runtimeClass.getName).withConsumers(
      consumers.map(_.asInstanceOf[CacheConsumerRegistration]).toSeq)

}

case object GetCacheConsumerInfo extends GetCacheConsumerInfo

trait GetCacheShardAssignmentInfo extends CacheRegistryMonitoringOperation {

  def request[E<:CDCEntity](group: String)(implicit ct:ClassTag[E]) = CacheShardAssignmentInfo.defaultInstance.
    withEntityFqn(ct.runtimeClass.getName).withGroupId(group)

  def response[E<:CDCEntity](group: String, assignments: Set[CacheShardRegistry.Assignment])(implicit ct:ClassTag[E]) =
    CacheShardAssignmentInfo.defaultInstance.withEntityFqn(ct.runtimeClass.getName).withGroupId(group).withAssignmentMap(
      assignments.groupBy(_._1.toSerializationFormat).mapValues(v => CacheShards.defaultInstance.withShards(v.map(_._2.asInstanceOf[CacheShardRegistration]).toSeq))
    )

}

case object GetCacheShardAssignmentInfo extends GetCacheShardAssignmentInfo

trait CacheRegistryMonitoringMessage[T<:CacheRegistryMonitoringOperation] extends InstrumentalMessage {
  type AnyM = E forSome {type E<:CacheRegistryMonitoringMessage[_]}

  lazy val messageType: T = {
    val token = TypeToken.of[AnyM](this.getClass.asInstanceOf[Class[AnyM]])
    val superToken = token.getSupertype(classOf[CacheRegistryMonitoringMessage[_]]).getType.asInstanceOf[ParameterizedType]
    val clazz =  TypeToken.of(superToken.getActualTypeArguments.head).getRawType.getName
    findCompanion[T](clazz, this.getClass.getClassLoader).get
  }

  override def getMessageType(): T = messageType

  val entityFqn: String

  lazy val entityClazz: Class[_<:CDCEntity] = Class.forName(entityFqn, true, getClass.getClassLoader).asInstanceOf[Class[_<:CDCEntity]]
}

trait CacheRegistryMonitoringMessageCompanion[E<:CacheRegistryMonitoringMessage[_]] extends InstrumentalMessageCompanion {
  override type M = E
}

private case object RebalanceCacheShardAssignments


/**
  * CacheRegistry cluster singleton actor
  */
class CacheShardRegistry extends Actor with ActorLogging {

  import CacheShardRegistry._

  import collection.JavaConverters._

  type CGroup = AtomicReference[(Int, Set[CacheConsumerRegistrationTrait])]
  type SGroup = AtomicReference[(Int, Set[CacheShardRegistrationTrait])]

  val rand = new scala.util.Random()

  implicit lazy val ec = AkkaCluster.miscEc

  // shards map
  val shardsMap: scala.collection.concurrent.TrieMap[String,  scala.collection.concurrent.TrieMap[String, SGroup]] =
    TrieMap[String,scala.collection.concurrent.TrieMap[String, SGroup]]()

  // consumers map
  val consumersMap: scala.collection.concurrent.TrieMap[String,  scala.collection.concurrent.TrieMap[String, CGroup]] =
    TrieMap[String,scala.collection.concurrent.TrieMap[String, CGroup]]()

  // allocations map
  val shardConsumerAssignmentsMap: scala.collection.concurrent.TrieMap[String,  scala.collection.concurrent.TrieMap[String,
    scala.collection.concurrent.TrieMap[String, Assignment]]] =
    TrieMap[String,scala.collection.concurrent.TrieMap[String,
      scala.collection.concurrent.TrieMap[String, Assignment]]]()

  override def preStart(): Unit = {
    super.preStart()
    val s = context.self
    context.system.scheduler.scheduleOnce(CACHE_SHARD_ASSIGNMENT_REBALANCE_PERIOD_SEC.seconds)(s ! RebalanceCacheShardAssignments)
  }


  private def getShards(entityFqn: String) = shardsMap.getOrElseUpdate(entityFqn, TrieMap[String, SGroup]())

  private def getShard(entityFqn: String, shard: String) = getShards(entityFqn).getOrElseUpdate(shard,
    new AtomicReference(getReplicationFactor(entityFqn) -> Set()))

  private def getGroups(entityFqn: String) = consumersMap.getOrElseUpdate(entityFqn, TrieMap[String, CGroup]())

  private def getGroup(entityFqn: String, group: String) = getGroups(entityFqn).getOrElseUpdate(group,
    new AtomicReference(0 -> Set()))

  private def getAllAssignments(entityFqn: String) =  shardConsumerAssignmentsMap.getOrElseUpdate(entityFqn,
    TrieMap[String, scala.collection.concurrent.TrieMap[String, Assignment]]())

  private def getGroupAssignments(entityFqn: String, group: String) = getAllAssignments(entityFqn).getOrElseUpdate(group,
    TrieMap[String, Assignment]())

  private def publishAssignments(entityFqn: String, assignmentsToSubscribe: Map[String, Set[Assignment]], assignmentsToUnsubscribe: Map[String, Set[Assignment]]) = {
    assignmentsToUnsubscribe.foldLeft[Set[Assignment]](Set())((x,a) => x ++ a._2).foreach(CacheShardRegistry.sendShardAssignment(_))
    assignmentsToSubscribe.foldLeft[Set[Assignment]](Set())((x,a) => x ++ a._2).foreach(CacheShardRegistry.sendShardAssignment(_))
    assignmentsToUnsubscribe.foreach(g => {log.debug(s"Published ${g._2} to group ${g._1}")})
    assignmentsToSubscribe.foreach(g => {log.debug(s"Published ${g._2} to group ${g._1}")})
  }

  private def flipAssignment(a: Assignment, status: CacheRegistryStatus): Assignment = a._1 -> a._2.flipStatus(status)


  override def receive: Receive = {
    case m: CacheConsumerRegistrationTrait => m.getMessageType match {
      case CACHE_STATUS_REGISTERING => {
        // new consumer registration
        val result = m.flipStatus(CACHE_STATUS_SUBSCRIBED)
        var next = getGroup(m.entityFqn, m.groupId).accumulateAndGet(m.parallelism -> Set(result),
          CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> (prev._2 ++ x._2)))
        val duplicates = next._2.groupBy(_.index)(m.index)
        if (duplicates.size > 1) {
          val removes = (duplicates - result)
          next = getGroup(m.entityFqn, m.groupId).accumulateAndGet(m.parallelism -> removes,
            CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> (prev._2 -- x._2)))
        }
        log.debug(s"Registered cache consumer $m")
      }
      case _ => {
        val consumerAssignedShards = getGroupAssignments(m.entityFqn, m.groupId).groupBy(_._2._1.toSerializationFormat).getOrElse(m.actorPath, Map()).keySet.toSeq
        // can directly unassign without publishing UNSUBSCRIBE messages back to the consumer because it wants to unregister and should be dead already
        consumerAssignedShards.foreach(unassign(m.entityFqn,m.groupId,_))
        val prev = getGroup(m.entityFqn, m.groupId).accumulateAndGet(m.parallelism -> Set(m),
          CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> {
            val indexToFind = x._2.head.index
            prev._2.find(indexToFind == _.index) match {
              case Some(z) => prev._2 - z
              case _ => prev._2
            }
          }))
        log.debug(s"Unregistered cache consumer $m")
        if (prev._2.isEmpty) {
          // remove the group
          getGroups(m.entityFqn).remove(m.groupId)
          getAllAssignments(m.entityFqn).remove(m.groupId)
        }
      }
    }
    case m: CacheShardRegistrationTrait => m.getMessageType match {
      case CACHE_STATUS_REGISTERING => {
        // new shard registration
        val replicationFactor = getReplicationFactor(m.entityFqn)
        val result = m.flipStatus(CACHE_STATUS_SUBSCRIBED)
        var next = getShard(m.entityFqn, m.shardId).accumulateAndGet(replicationFactor -> Set(result),
          CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> (prev._2 ++ x._2)))
        log.debug(s"Registered cache shard $m")
        val duplicates = next._2.groupBy(_.replica)(m.replica)
        if (duplicates.size >1) {
          val removes = (duplicates - result)
          next = getShard(m.entityFqn, m.shardId).accumulateAndGet(replicationFactor -> removes,
            CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> (prev._2 -- x._2)))
        }
      }
      case CACHE_STATUS_SUBSCRIBED => {
        // consumer has subscribed to shard
        val senderPath = sender.path
        val rootAddress = senderPath.root.address
        val address = rootAddress.host match {
          case Some(_) => rootAddress
          case _ => CacheShardRegistry.address
        }
        val senderPathString = senderPath.toStringWithAddress(address)
        getAllAssignments(m.entityFqn).mapValues(_.get(m.shardId))
          .filter(v => v._2.isDefined && v._2.get._1.toSerializationFormat == senderPathString).foreach(c => {
           assign(m.entityFqn, c._1, flipAssignment(c._2.get, CACHE_STATUS_SUBSCRIBED))
        })
        log.debug(s"Consumer $senderPathString has subscribed to cache shard $m")
      }

      case CACHE_STATUS_UNREGISTERING => {
        // shard is un-registering
        val replicationFactor = getReplicationFactor(m.entityFqn)
        val prev = getShard(m.entityFqn, m.shardId).accumulateAndGet(replicationFactor -> Set(m),
          CacheShardRegistry.toJavaBiOperator((prev, x) => x._1 -> {
            val replicaToFind = x._2.head.replica
            prev._2.find(replicaToFind == _.replica) match {
              case Some(z) => prev._2 - z
              case _ => prev._2
            }
          }))
        log.debug(s"Unregistered cache shard $m")
        if (prev._2.isEmpty) {
          // remove the shard
          getShards(m.entityFqn).remove(m.shardId)
         }
      }

      case _ => {
        // shard has been unsubscribed - remove assignment
        val senderPath = sender.path
        val rootAddress = senderPath.root.address
        val address = rootAddress.host match {
          case Some(_) => rootAddress
          case _ => CacheShardRegistry.address
        }
        val senderPathString = senderPath.toStringWithAddress(address)
        getAllAssignments(m.entityFqn).mapValues(_.get(m.shardId))
          .filter(v => v._2.isDefined && v._2.get._1.toSerializationFormat == senderPathString).foreach(c => {
          unassign(m.entityFqn, c._1, m.shardId)
        })
        log.debug(s"Consumer $senderPathString has un-subscribed to cache shard $m")
      }

    }

    // triggers period rebalancing of allocations

    case RebalanceCacheShardAssignments => {
      shardsMap.keys.foreach(e => {
        getGroups(e).foreach(g => {
          val curr = g._2.get()
          if (curr._1 <= curr._2.size) {
            val diff = rebalanceGroupAssignments(e, g._1)
            if (diff._1.isEmpty && diff._2.isEmpty){} else {
              publishAssignments(e, Map(g._1 -> diff._1), Map(g._1 -> diff._2))
            }
          }
        })
      })
      val s = context.self
      context.system.scheduler.scheduleOnce(CACHE_SHARD_ASSIGNMENT_REBALANCE_PERIOD_SEC.seconds)(s ! RebalanceCacheShardAssignments)
    }

    case m: CacheRegistryMonitoringMessage[_] => m.getMessageType match {
      case GetCacheShardInfo => {
        sender ! GetCacheShardInfo.response(getShards(m.entityFqn).values.foldLeft[Set[CacheShardRegistrationTrait]](Set())
          ((x,s) => x ++ s.get()._2))(ClassTag(m.entityClazz))
      }
      case GetCacheConsumerInfo => {
        sender ! GetCacheConsumerInfo.response(getGroups(m.entityFqn).foldLeft[Set[CacheConsumerRegistrationTrait]](Set())((x,s) => {
          x ++ s._2.get()._2
        }))(ClassTag(m.entityClazz))
      }
      case GetCacheShardAssignmentInfo => {
        val groupId = m.asInstanceOf[CacheShardAssignmentInfo].groupId
        sender ! GetCacheShardAssignmentInfo.response(groupId, getGroupAssignments(m.entityFqn, groupId).values.toSet)(ClassTag(m.entityClazz))
      }
    }
  }

  // assignment logic: assign shards to consumer running on the same node, if it exists - otherwise uniform random to all consumers
  private def rebalanceGroupAssignments(entityFqn: String, group: String): (Set[Assignment], Set[Assignment]) = {
    val oldAssignments = getGroupAssignments(entityFqn, group).values.toSet
    val consumerPaths = getGroup(entityFqn,group).get()._2.map(_.actorPath)
    val validAssignments = oldAssignments.filter(a => (
      CACHE_STATUS_SUBSCRIBED == a._2.status
      && consumerPaths.contains(a._1.toSerializationFormat)
      && getShard(entityFqn,a._2.shardId).get()._2.contains(a._2)))
    val validShards = validAssignments.map(_._2.shardId)
    val shardsToAssign = getShards(entityFqn).keys.filter(!validShards.contains(_)).toSet
    val newAssignments = shardsToAssign.filter(s => {
      val sg = getShard(entityFqn,s).get()
      sg._1 <= sg._2.size
    }).map(computeAssignment(entityFqn,group,_)).map(flipAssignment(_, CACHE_STATUS_REGISTERING))
    shardsToAssign.foreach(unassign(entityFqn, group, _))
    newAssignments.foreach(assign(entityFqn, group, _))
    (newAssignments, (oldAssignments diff validAssignments).map(flipAssignment(_, CACHE_STATUS_UNREGISTERING)))
  }

  private def computeAssignment(entityFqn: String, group: String, shard: String): Assignment = {
    val cgroup: Map[String, Set[CacheConsumerRegistrationTrait]] = getGroup(entityFqn,group).get._2.groupBy(_.hostname)
    val sgroup: Seq[CacheShardRegistrationTrait] = getShard(entityFqn, shard).get._2.toSeq.sortBy(_.replica).reverse
    val r: (CacheShardRegistrationTrait, Seq[CacheConsumerRegistrationTrait]) = sgroup.find(s => cgroup.get(s.hostname).isDefined) match {
       case Some(s) => s -> cgroup(s.hostname).toSeq
       case _ => sgroup.head -> getGroup(entityFqn,group).get._2.toSeq
    }
    r._2(rand.nextInt(r._2.size)).consumerActorSelection -> r._1
  }

  private def unassign(entityFqn: String, group: String, shard: String): Option[Assignment] = {
    getGroupAssignments(entityFqn, group).remove(shard)
  }

  private def assign(entityFqn: String, group: String, assignment: Assignment): Option[Assignment] = {
    getGroupAssignments(entityFqn, group).put(assignment._2.shardId, assignment)
  }
}

object CacheShardRegistry {

  type Assignment = (ActorSelection, CacheShardRegistrationTrait)

  protected lazy val logger: Logger =
    Logger(LoggerFactory.getLogger(classOf[CacheShardRegistry]))

  def getReplicationFactor(entityFqn: String) = CacheShardExtension.config.get(entityFqn).map(_.replication).getOrElse(1)

  private lazy val CACHE_SHARD_ASSIGNMENT_REBALANCE_PERIOD_SEC =  AkkaCluster.configurationProvider
    .getConfig.getInt("cache.coordinator.period")

  val props = Props.apply(classOf[CacheShardRegistry]).withDispatcher("dispatchers.affinity")

  private lazy val address = AkkaCluster.selfAddress

  def cacheConsumerRegistration[E<:CDCEntity](status: CacheRegistryStatus,
                                              consumerActor: ActorRef, group: String, parallelism: Int, index: Int)
                                             (implicit ct: ClassTag[E]): CacheConsumerRegistrationTrait = {
    innerCacheConsumerRegistration(ct.runtimeClass.getName, status, consumerActor.path.toStringWithAddress(address), group, address.host.get, parallelism, index)

  }

  def cacheShardRegistration[E<:CDCEntity](status: CacheRegistryStatus,
                                           shard: String, replica: Int)
                                          (implicit ct: ClassTag[E]): CacheShardRegistrationTrait = {
    innerCacheShardRegistration(ct.runtimeClass.getName, status, shard, address.host.get, replica)

  }

  private def innerCacheShardRegistration(entityFqn: String, status: CacheRegistryStatus,
                                          shard: String, hostname: String, replica: Int) = {
    CacheShardRegistration.defaultInstance
      .withEntityFqn(entityFqn)
      .withShardId(shard)
      .withHostname(hostname)
      .withReplica(replica)
      .withStatus(status.asInstanceOf[com.cachakka.streaming.akka.shard.cache.CacheRegistryStatusEnum])
  }

  private def innerCacheConsumerRegistration(entityFqn: String, status: CacheRegistryStatus,
                                     actorPath: String, group: String, hostname: String, parallelism: Int, index: Int) = {
    CacheConsumerRegistration.defaultInstance
      .withActorPath(actorPath).withEntityFqn(entityFqn)
      .withGroupId(group).withHostname(hostname)
      .withParallelism(parallelism)
      .withIndex(index)
      .withStatus(status.asInstanceOf[com.cachakka.streaming.akka.shard.cache.CacheRegistryStatusEnum])
  }

  private def sendShardAssignment(assignment: Assignment): Unit = {
    assignment._1 ! assignment._2
  }

  implicit def toJavaBiFunction[X,Y,Z](f: Function2[X,Y,Z]): BiFunction[X,Y,Z] = new BiFunction[X,Y,Z]{

    override def apply(x: X, y: Y): Z = f(x,y)

    override def andThen[V](after: java.util.function.Function[_ >: Z, _ <: V]): BiFunction[X, Y, V] =
      toJavaBiFunction((x:X,y:Y) => after.apply(f(x,y)))
  }

  implicit def toJavaBiOperator[X](f: Function2[X,X,X]) = new BinaryOperator[X]{

    override def apply(x: X, y: X): X = f(x,y)

    override def andThen[V](after: java.util.function.Function[_ >: X, _ <: V]): BiFunction[X, X, V] =
      toJavaBiFunction((x:X,y:X) => after.apply(f(x,y)))
  }


}