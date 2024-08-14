package com.cachakka.streaming.akka.grpc




import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, PoisonPill, Props}
import com.trueaccord.scalapb.grpc.{AbstractService, ServiceCompanion}
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.akka._
import com.cachakka.streaming.akka.ServerHealthcheckApiGrpc.ServerHealthcheckApi
import com.cachakka.streaming.akka.ShardingRegionManagementApiGrpc.ShardingRegionManagementApi
import io.grpc._
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success


/**
  * Meta-data about a registered gRPC service (which is a collection of gRPC APIs implemented and deployed together)
  */
trait GrpcServiceCompanion{

  type A<:AbstractService

  val descriptor: ServiceCompanion[A]

  def prepareServer(implicit ec: ExecutionContext): ServerServiceDefinition

}

trait GrpcServerHealthcheckHandler extends ServerHealthcheckApi with LazyLogging{
  override def ping(request: GrpcCheckAlive): Future[GrpcCheckAlive] = {
    logger.debug(s"gRPC healthcheck service received a ping")
    Future.successful(request)
  }
}

object GrpcServerHealthcheckHandlerImpl extends GrpcServerHealthcheckHandler

trait GrpcServerHealthcheckService extends GrpcServiceCompanion{

  override type A = ServerHealthcheckApi

  override val descriptor = ServerHealthcheckApi

  override def prepareServer(implicit ec: ExecutionContext): ServerServiceDefinition =
    ServerHealthcheckApiGrpc.bindService(GrpcServerHealthcheckHandlerImpl,ec)
}

object GrpcServerHealthcheckService extends GrpcServerHealthcheckService

trait GrpcShardingRegionManagementHandler extends ShardingRegionManagementApi with LazyLogging{
  override def restart(request: GrpcRestartShardingRegion): Future[GrpcRestartShardingRegion] = {
    logger.info(s"gRPC sharding region management service received a restart request")
    AkkaCluster.restartShardingEntityServicesRegionActors
    Future.successful(request)
  }
}

object GrpcShardingRegionManagementHandlerImpl extends GrpcShardingRegionManagementHandler

trait GrpcShardingRegionManagementService extends GrpcServiceCompanion{

  override type A = ShardingRegionManagementApi

  override val descriptor = ShardingRegionManagementApi

  override def prepareServer(implicit ec: ExecutionContext): ServerServiceDefinition =
    ShardingRegionManagementApiGrpc.bindService(GrpcShardingRegionManagementHandlerImpl,ec)
}

object GrpcShardingRegionManagementService extends GrpcShardingRegionManagementService


private case object ServerSelfCheck

/**
  * JVM-singleton actor used to host an instance of a gRPC server per node
  */
class GRPCDeployActor extends Actor with ActorLogging{

  import scala.collection.JavaConverters._

  import scala.concurrent.duration._

  private var serverOption: Option[Server] = None



  lazy val channel = {
    val c = GrpcServiceExtension.serviceRegistry.get(ServerHealthcheckApi.javaDescriptor.getName()).get
    ManagedChannelBuilder
      .forAddress(c.hostname, c.port).asInstanceOf[ManagedChannelBuilder[NettyChannelBuilder]]
      .usePlaintext(true)
      .keepAliveTime(1, TimeUnit.MINUTES)
      .keepAliveWithoutCalls(true)
      .build
  }

  override def preStart(): Unit = {
    implicit val ec = context.system.dispatchers.lookup("dispatchers.misc")
    super.preStart()
    import scala.collection.JavaConverters._
    val server = GrpcServiceExtension.serviceRegistry.values.foldLeft[ServerBuilder[_]](ServerBuilder.forPort(
      AkkaCluster.configurationProvider.getConfig.getInt("grpc.port")))((x,s) => x.addService(s.companion.prepareServer).asInstanceOf[ServerBuilder[_]])
      .build().start()
    log.info(s"Started Akka gRPC server on port ${server.getPort} with immutable services ${server.getImmutableServices.asScala.map(_.getServiceDescriptor.getName)} " +
      s"and mutable services ${server.getMutableServices.asScala.map(_.getServiceDescriptor.getName)}")
    serverOption = Some(server)
    val s = context.self
    context.system.scheduler.scheduleOnce(10.seconds)(s ! ServerSelfCheck)
  }

  override def postStop(): Unit = {
    val server = serverOption.get
    channel.shutdownNow()
    channel.awaitTermination(10, TimeUnit.SECONDS)
    server.shutdownNow()
    server.awaitTermination(10, TimeUnit.SECONDS)
    log.info(s"Stopped Akka gRPC server running on on port ${server.getPort} with immutable services ${server.getImmutableServices.asScala.map(_.getServiceDescriptor.getName)} " +
      s"and mutable services ${server.getMutableServices.asScala.map(_.getServiceDescriptor.getName)}")
    super.postStop()
  }


  override def receive: Receive = {
    case ServerSelfCheck => if (serverOption.get.isTerminated) {
      context.self ! PoisonPill
    } else {
      implicit val ec = context.system.dispatchers.lookup("dispatchers.misc")
      val s = context.self
      val client = new ServerHealthcheckApiGrpc.ServerHealthcheckApiStub(channel, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS))
      client.ping(GrpcCheckAlive.defaultInstance).onComplete(_ match {
        case x: Throwable  => {
          log.error(s"Failed healthcheck on gRPC server running on port ${serverOption.get.getPort}")
          s ! PoisonPill
        }
        case _ => log.debug(s"Normal healthcheck on gRPC server running on port ${serverOption.get.getPort}")
      })
      context.system.scheduler.scheduleOnce(10.seconds)(s ! ServerSelfCheck)
    }
  }

}


class GrpcServiceExtensionImpl(actorSystem: ActorSystem) extends Extension with LazyLogging{

    val serverDeployActor = actorSystem.actorOf(Props.apply[GRPCDeployActor]().withDispatcher("dispatchers.affinity"))
}

case class GrpcServiceConfig(val companion: GrpcServiceCompanion, hostname: String, port: Int)

/**
  * Akka gRPC externsion: starts gRPC servers when actor system starts
  */
object GrpcServiceExtension
  extends ExtensionId[GrpcServiceExtensionImpl]
    with ExtensionIdProvider {

  /**
    * Simple service registry. Can be used by fat/load-balancing gRPC clients to discover all hosts running a service instance
    */
  lazy val serviceRegistry: Map[String, GrpcServiceConfig] = {
    val config = AkkaCluster.configurationProvider.getConfig.getConfig("grpc")
    config.getStringList("services").asScala.map(c => {
      val cmp: GrpcServiceCompanion = findCompanion[GrpcServiceCompanion](c, Thread.currentThread().getContextClassLoader) match {
        case Success(v) => v
        case _ => Class.forName(c).asInstanceOf[Class[_<:GrpcServiceCompanion]].newInstance()
      }
      cmp.descriptor.javaDescriptor.getName -> GrpcServiceConfig(cmp, "localhost", config.getInt("port"))
    }).toMap
  }

  override def lookup = GrpcServiceExtension

  override def createExtension(system: ExtendedActorSystem) = new GrpcServiceExtensionImpl(system)

  override def get(system: ActorSystem): GrpcServiceExtensionImpl = super.get(system)

}
