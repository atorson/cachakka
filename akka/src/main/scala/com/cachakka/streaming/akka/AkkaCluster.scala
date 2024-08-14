package com.cachakka.streaming.akka

import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

import akka.actor.{ActorRef, ActorSystem, Address, PoisonPill}
import akka.cluster.{Cluster, UniqueAddress}
import akka.cluster.sharding.ShardCoordinator.{LeastShardAllocationStrategy, ShardAllocationStrategy}
import akka.cluster.sharding.ShardRegion.ShardId
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings, ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern.{AskTimeoutException, Patterns}
import akka.util.Timeout
import com.google.common.base.Strings
import com.google.inject.{Guice, Module}
import com.cachakka.streaming.configuration.{ConfigurationProvider, InjectableClasspathResourceConfigProvider}
import org.joda.time.DateTime

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

object AkkaCluster {

  import scala.concurrent.duration._

  val MANAGER_ROLE = "cachemanager"

  val WORKER_ROLE = "cacheworker"

  val AKKA_SYSTEM_NAME = "flink-colo-akka"

  val SHARD_RECEIVE_TIMEOUT_SEC = 3600000

  val SHARDING_ENTITY_SERVICES = "sharded-entity-services"

  val CLUSTER_HTTP_MANAGEMENT_SERVICE = "cluster-http-management-service"

  val configurationProvider: ConfigurationProvider = {
    val role = System.getProperty("targetRole")
    val env = System.getProperty("targetEnvironment")
    if (Strings.isNullOrEmpty(role) || Strings.isNullOrEmpty(env)) throw new RuntimeException("System properties 'targetRole' and/or 'targetEnvironment' are not set")
    new InjectableClasspathResourceConfigProvider(s"$role-$env.conf", true)
  }

  private val seedHosts = {
    import scala.collection.JavaConverters._
    configurationProvider.getConfig.getStringList("akka.cluster.seed-nodes").asScala.toSet.filterNot(_.contains("localhost"))
  }

  private val actorSystemStartupFuture: AtomicReference[Future[Unit]] = new AtomicReference()

  private class ShardEntityExtractor extends ShardRegion.MessageExtractor {
    override def entityId(message: Any): String = message match {
        case m: ShardedEntity => m.getEntityID
        case _ => null
      }


    override def shardId(message: Any): String = message match {
      case m: ShardedEntity => m.getShardID
      case _ => null
    }

    override def entityMessage(message: Any): Any =
      message match {
        case m: ShardedEntity => m
        case _ => null
      }
  }

  val shardingMessageExtractor: ShardRegion.MessageExtractor = new ShardEntityExtractor

  val shardAllocationStrategy: ShardAllocationStrategy = {
    val config = configurationProvider.getConfig.getConfig("akka.cluster.sharding.least-shard-allocation-strategy")
    new HybridAkkaShardAllocationStrategy(config.getInt("rebalance-threshold"), config.getInt("max-simultaneous-rebalance"))
  }

  /**
    * For each sharding service (we have CDC and Cache now), ShardRegion actor serves both as shard host and as a proxy to communicate with any shard
    */
  val shardingEntityServicesRegionActor: AtomicReference[Map[String,ActorRef]] = new AtomicReference()

  /**
    * Our HTTP servers (hosting Swagger APIs) are run as a cluster singleton - and the SingletonActor serves both as singleton host and as a proxy to communicate with it
    */
  val clusterHttpManagementSingletonActor: AtomicReference[ActorRef] = new AtomicReference()

  /**
    * Custom callbacks to be executed at different lifecycles transitions of the cluster membership
    * The integer keys reflect a priority/order in which they are executed
    */
  val onMemberUpCallbacks = TrieMap[Int, () => Unit](
    10 -> (() => shardingEntityServicesRegionActor.set(innerStartShardingEntityServicesRegionActors)),
    20 -> (() => clusterHttpManagementSingletonActor.set(innerStartClusterHttpManagementSingleton)))

  val onMemberRemovedCallbacks = TrieMap[Int, () => Unit](
    10 -> (() => shutdownAkka))

  val onTerminationCallbacks = TrieMap[Int, () => Unit](
    10 -> (() => {
      System.out.println(s"Graceful termination of JVM at time ${new DateTime(System.currentTimeMillis())}")
      System.exit(0)
    })
  )

  /**
    * 100% reliable method to shutdown Akka system and then JVM
    */
  private def shutdownAkka: Unit = {
    val s = AkkaCluster.actorSystem
    new Thread() {
      override def run(): Unit = {
        try
          Await.result(s.whenTerminated, 5.seconds)
        catch {
          case _: Throwable => {
            System.out.println(s"Forceful termination of JVM at time ${new DateTime(System.currentTimeMillis())}")
            System.exit(-1)
          }
        }
      }
    }.start()
    s.terminate
  }

  /**
    * This is the single most important object representing Akka actor system running in this JVM
    */
  var actorSystem = innerCreateActorSystem

  /**
    * This method can be called many times, restarting Akka actor system each time
    * @return
    */
  protected def innerCreateActorSystem = {

    val s = ActorSystem.create(AkkaCluster.AKKA_SYSTEM_NAME, configurationProvider.getConfig)

    s.registerOnTermination {
      actorSystemStartupFuture.set(null)
      onTerminationCallbacks.toSeq.sortBy(_._1).foreach(_._2.apply())
    }

    val cluster = Cluster.get(s)

    cluster.registerOnMemberRemoved{onMemberRemovedCallbacks.toSeq.sortBy(_._1).foreach(_._2.apply())}

    val p = Promise[Unit]()

    s.scheduler.scheduleOnce(20.minute)(p tryFailure (
      new AskTimeoutException(s"Timed out after 20 minutes on node ${selfAddress.host.getOrElse("")} trying to join the $AKKA_SYSTEM_NAME cluster")
    ))(s.dispatchers.lookup("dispatchers.misc"))

    cluster.registerOnMemberUp{
      onMemberUpCallbacks.toSeq.sortBy(_._1).foreach(_._2.apply())
      p trySuccess ()
    }

    p.future.onFailure{case _: Throwable => {
      System.out.println(s"-----FATAL: Could not join the $AKKA_SYSTEM_NAME at time ${new DateTime(System.currentTimeMillis())} cluster after 5 minutes wait, terminating Akka-------")
      shutdownAkka
    }}((s.dispatchers.lookup("dispatchers.misc")))

    actorSystemStartupFuture.set(p.future)
    s
  }

  lazy val selfAddress: Address = Cluster.get(AkkaCluster.actorSystem).selfUniqueAddress.address

  /**
    * General dispatchers that can be used in any multi-threaded operations
    */
  val miscEc = actorSystem.dispatchers.lookup("dispatchers.misc")

  val blockingEc = actorSystem.dispatchers.lookup("dispatchers.bulkheading")

  val expressEC = actorSystem.dispatchers.lookup("dispatchers.affinity")

  val shardingSettings = ClusterShardingSettings.create(actorSystem)

  protected def innerStartClusterHttpManagementSingleton: ActorRef = {
    val roles = Cluster.get(actorSystem).selfRoles
    if (roles.contains(AkkaCluster.MANAGER_ROLE)) {
      actorSystem.actorOf(ClusterSingletonManager.props(ClusterHttpManagementActor.props, PoisonPill,
        ClusterSingletonManagerSettings.apply(actorSystem).withRole(AkkaCluster.MANAGER_ROLE)), AkkaCluster.CLUSTER_HTTP_MANAGEMENT_SERVICE)
    }
    actorSystem.actorOf(ClusterSingletonProxy.props("/user/" + AkkaCluster.CLUSTER_HTTP_MANAGEMENT_SERVICE, ClusterSingletonProxySettings.apply(actorSystem)
      .withRole(AkkaCluster.MANAGER_ROLE)), AkkaCluster.CLUSTER_HTTP_MANAGEMENT_SERVICE + "-proxy")
  }

  protected def innerStartShardingEntityServicesRegionActors: Map[String,ActorRef] = {
    AkkaCluster.injector.getInstance(classOf[ShardedEntityOperationProvider]).operations.map(o => {
      o.getServiceName -> ClusterSharding.get(actorSystem).start(s"${AkkaCluster.SHARDING_ENTITY_SERVICES}_${o.getServiceName}",
        ShardedEntityActor.props, shardingSettings, shardingMessageExtractor, shardAllocationStrategy, PoisonPill)
    }).toMap
  }

  def restartShardingEntityServicesRegionActors: Unit = shardingEntityServicesRegionActor.set(innerStartShardingEntityServicesRegionActors)

  lazy val injector = {
    import scala.collection.JavaConverters._
    Guice.createInjector(configurationProvider.getConfig.getStringList("guice.modules").asScala
      .map(Class.forName(_).newInstance().asInstanceOf[Module]).asJava)
  }

  /**
    * High-throughput Ask-pattern implementation client
    * instead of using Akka 'ask' method - use this object's API
    */
  lazy val shardAskManger = new ShardAskManager(900.seconds, 30.seconds)

  /**
    * DON'T USE this API in high-throughput contexts. It is blocking.
    * Good for tests and ad-hoc API calls
    * @param m
    * @param timeout
    * @return
    */
  def syncCallShardingService(m: ShardedEntity, timeout: FiniteDuration = 10.seconds): Try[ClusterMessage] = {
    Try{Await.result(asyncCallShardingService(m), timeout)}
  }

  /**
    * This is the main async-IO API to be used to invoke any sharding service
    * Suitable for high-throughput contexts. Allows to use Future-composition to continue processing when response comes back
    * @param m
    * @param timeout optional, there is a default buil-in coarse timeout mechanism.
    * Don't specify it in high-throughput contexts, as per-call timeouts come with a cost (as opposed to built-in coarse timeout
    * provided as a AskManager constructor parameter)
    * @return Always tries to produce an answer, even in bad situations (it will be a Nack then). Future may be a failure in case of timeouts though
    */
  def asyncCallShardingService(m: ShardedEntity, timeout: Option[FiniteDuration] = None): Future[ClusterMessage] = {
    implicit lazy val ec = miscEc
    m match {
      case u: ShardedEntity with AsyncAskMessage[_] => shardAskManger.asyncCallShardingService(u, timeout)
      case _  =>  {
        val message: ShardedEntity = m match {
          case v: AutoShardedEntity[_] => v.processShardId(m.getOperation)
          case _ => m
        }
        Patterns.ask(AkkaCluster.shardingEntityServicesRegionActor.get()(message.getOperation.getServiceName), message, Timeout(timeout.getOrElse(Integer.MAX_VALUE.seconds)))
          .map(_.asInstanceOf[ClusterMessage])
      }
    }
  }

  /**
    * Fire-and-forget sharding service invokation API. Ultra-high throughput. No response is expected back
    * @param m
    * @param sender
    */
  def messageShardingService(m: ShardedEntity)(implicit sender: ActorRef): Unit = {
    val message: ShardedEntity = m match {
      case v: AutoShardedEntity[_] => v.processShardId(m.getOperation)
      case _ => m
    }
    AkkaCluster.shardingEntityServicesRegionActor.get()(message.getOperation.getServiceName) ! message
  }

  /**
    * Blocking API forcing the caller to wait until this JVM joins the Akka cluster fully. Can be used in Flink operator initialization routines
    */
  def blockUntilMemeberUp(): Unit = {
    var f = actorSystemStartupFuture.get()
    if (f == null) {
      actorSystem = innerCreateActorSystem
      f = actorSystemStartupFuture.get()
    }
    if (!f.isCompleted) Await.result(f, 5.minutes)
  }

  /**
    * Shard allocation strategy. Uses uniform random allocation unless shard name contains an IP address: in this case it picks the node with this IP
    * @param rebalanceThreshold
    * @param maxSimultaneousRebalance
    */
  class HybridAkkaShardAllocationStrategy(rebalanceThreshold: Int, maxSimultaneousRebalance: Int) extends LeastShardAllocationStrategy(rebalanceThreshold, maxSimultaneousRebalance)  {

    implicit lazy val ec = AkkaCluster.miscEc

    private val pattern = Pattern.compile("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")

    private val numeric = Set[String]("0","1","2","3","4","5","6","7","8","9")

    private def findAddressStart(input: String): Option[Int] = {
      val index = input.indexOf(".")
      val size = scala.math.min(3,index)
      if (index > 0){
      Range(0, size).find(i => !numeric.contains(input.substring(index - i -1, index - i)))  match {
        case Some(r) => Some(index - r)
        case _ => Some(index - size)
      }} else None
    }

    private def findAddressEnd(input: String): Option[Int] = {
      val index = input.lastIndexOf(".")
      val size = scala.math.min(3,(input.size - 1 - index))
      if (index < (input.size -1)){
        Range(0, size).find(i => !numeric.contains(input.substring(index + i + 1, index + i + 2)))  match {
          case Some(r) => Some(index + r)
          case _ => Some(index + size)
        }} else None
    }

    private def getLocality(input: ShardId): Option[String]  = (findAddressStart(input), findAddressEnd(input)) match {
      case (Some(start), Some(end)) => {
        val matcher =  pattern.matcher(input.substring(start, end + 1))
        if (matcher.find()){
          Some(matcher.group())
        } else None
      }
      case _ => None
    }

    override def allocateShard(requester: ActorRef, shardId: ShardId,
                               currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): Future[ActorRef] = {
      val filteredAllocations = currentShardAllocations.filterNot(x => seedHosts.find(_.contains(x._1.path.root.address.host.getOrElse(selfAddress.host.get))).isDefined)
      getLocality(shardId) match {
        case Some(address) => filteredAllocations.find(e => getLocality(e._1.path.root.address.host.getOrElse(AkkaCluster.selfAddress.host.get)).filter(_ == address).isDefined) match {
          case Some((a,_)) => Future.successful(a)
          case _ => throw new RuntimeException(s"Could not match any ShardRegion actor in ${filteredAllocations.keySet.map(_.path)} to the shard $shardId address $address")
        }
        case _ => super.allocateShard(requester, shardId, filteredAllocations)
      }
    }

    override def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                           rebalanceInProgress:     Set[ShardId]): Future[Set[ShardId]] =
      super.rebalance(currentShardAllocations.filterNot(x => seedHosts.find(_.contains(x._1.path.root.address.host.getOrElse(selfAddress.host.get))).isDefined),
        rebalanceInProgress).map(_.filter(y => getLocality(y).isEmpty))


  }
}





