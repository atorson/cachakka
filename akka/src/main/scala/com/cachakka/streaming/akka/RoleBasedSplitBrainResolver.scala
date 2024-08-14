package com.cachakka.streaming.akka


import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, Address, Cancellable, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus.{Down, Exiting}
import akka.cluster._
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.akka.RoleBasedSplitBrainResolver._

import scala.collection.immutable

import scala.concurrent.duration.{Deadline, DurationInt, FiniteDuration}


final class RoleBasedSplitBrainResolverProvider(system: ActorSystem) extends DowningProvider {

  override def downRemovalMargin: FiniteDuration = FiniteDuration(system.settings.config.getDuration("akka.cluster.down-removal-margin").toMillis, TimeUnit.MILLISECONDS);

  override def downingActorProps: Option[Props] = Some(RoleBasedSplitBrainResolver.props)
}

object RoleBasedSplitBrainResolver {

  val props: Props = Props(classOf[RoleBasedSplitBrainResolver])

  sealed trait DownAction
  case object DownReachable extends DownAction
  case object DownUnreachable extends DownAction
  case object DownAll extends DownAction

  case object Tick
}

/**
  * Split brain resolver that is used to kick out unresponsive/unobservable nodes out of the cluster and also resolve rare split-brain scenarios
  */
class RoleBasedSplitBrainResolver() extends Actor with LazyLogging {

  import context.dispatcher

  val cluster = Cluster(context.system)

  val essentialRoles = cluster.settings.MinNrOfMembersOfRole.filter(_._2 >0)

  val firstSeedNode = cluster.settings.SeedNodes.head

  def isLeader(nodes: Set[Address]) = {
    val firstSeedNodeOption = cluster.settings.SeedNodes.filter(nodes.contains(_)).headOption
    firstSeedNodeOption.filter(_ == selfUniqueAddress.address).isDefined
  }

  def containsAllLeaders(nodes: Set[Address]) = cluster.settings.SeedNodes.filterNot(nodes.contains(_)).isEmpty

  val stableAfter = FiniteDuration(cluster.settings.config.getDuration("akka.cluster.stable-after").toMillis, TimeUnit.MILLISECONDS);

  val selfUniqueAddress = cluster.selfUniqueAddress

  var tickTask: Cancellable = null

  private def resetDeadline = {stableDeadline = Deadline.now + stableAfter}

  var stableDeadline: Deadline = null

  var unreachable: Set[UniqueAddress] = Set()

  private var members: Set[Member] = Set()

  def unreachableMembers = members.filter(m => unreachable(m.uniqueAddress))

  def reachableMembers = members.filter(m => !unreachable(m.uniqueAddress))

  override def preStart(): Unit = {
    resetDeadline
    val a = self
    tickTask = context.system.scheduler.schedule(stableAfter, 5.seconds, a, Tick)
    super.preStart()
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    tickTask.cancel()
    super.postStop()
  }

 def decide(r: Set[Address], u: Set[Address], reachableOK: Boolean, unreachableOK: Boolean): Set[Address] =
    if (containsAllLeaders(u)) {
      logger.info(s"Reachable nodes are leader-less: self-downing ${selfUniqueAddress.address}")
      Set(selfUniqueAddress.address)
    } else if (isLeader(r)){
      val decision = if (unreachableOK && reachableOK) {
        // If both sides contains essential roles, then use the side that has 1st seed node (to avoid islanding after restarts)
        if (u.find(_ == firstSeedNode).isDefined) {
          logger.info("Unreachable nodes have the 1st seed node, DownReachable")
          DownReachable
        } else {
          logger.info("Reachable nodes have the 1st seed node, DownUnreachable")
          DownUnreachable
        }
      } else if (unreachableOK) {
        logger.info("Only unreachable nodes have essential roles, DownReachable")
        DownReachable
      } else if (reachableOK) {
        logger.info("Only reachable nodes have essential roles, DownUnreachable")
        DownUnreachable
      } else {
        logger.info("No side has essential roles, DownAll")
        DownAll
      }
      decision match {
        case DownUnreachable =>  u
        case DownReachable => r
        case DownAll => u ++ r
        case _ => Set()
      }
  } else Set()

  def receive = {

    case Tick => {

      val newMembers = Set() ++ cluster.state.members
      val newUnreachable = Set() ++ cluster.state.unreachable.map(_.uniqueAddress)

      val stable = (newUnreachable == unreachable) && (newMembers.groupBy(_.status) == members.groupBy(_.status))

      if (!stable) {
        members = newMembers
        unreachable = newUnreachable
        resetDeadline
      } else {

        val r = reachableMembers
        val u = unreachableMembers

        val reachableOK = essentialRoles.filterNot(e => e._2 > r.filter(_.roles.contains(e._1)).size).size == essentialRoles.size
        val unreachableOK = essentialRoles.filterNot(e => e._2 > u.filter(_.roles.contains(e._1)).size).size == essentialRoles.size
        val selfPresent = members.find(_.uniqueAddress == selfUniqueAddress).isDefined

        val shouldAct = selfPresent && stableDeadline.isOverdue() && (unreachable.nonEmpty || !reachableOK)

        if (shouldAct) {

          logger.info(s"Brain-split resolver run: reachableOK = $reachableOK, unreachableOK = $unreachableOK, reachable size = ${r.size}, unreachable nodes: $unreachable")

          val downNodes = decide(r.map(_.address),u.map(_.address), reachableOK, unreachableOK)

          logger.info(s"Brain-split resolver downing decision: $downNodes")

          downNodes.foreach(cluster.down(_))

          resetDeadline
        }
      }
    }

    case _: ClusterDomainEvent ⇒ // ignore

  }

}