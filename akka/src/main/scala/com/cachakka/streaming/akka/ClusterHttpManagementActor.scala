package com.cachakka.streaming.akka

import java.util.concurrent.TimeUnit
import javax.ws.rs.Path

import akka.actor.{Actor, ActorLogging, ActorSystem, AddressFromURIString, ExtendedActorSystem, ExtensionId, ExtensionIdProvider, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.sharding.{ClusterSharding, ShardRegion}
import akka.http.scaladsl.server.{Directive0, Route, RouteConcatenation}
import akka.management.AkkaManagement
import akka.management.cluster._
import akka.management.http.{ManagementRouteProvider, ManagementRouteProviderSettings}
import akka.pattern.{AskTimeoutException, Patterns}
import akka.http.scaladsl.model.headers.{`Access-Control-Allow-Credentials`, `Access-Control-Allow-Headers`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Origin`}
import akka.http.scaladsl.server.Directives.{complete, mapResponseHeaders, options}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshalling.ToResponseMarshallable.apply
import akka.http.scaladsl.model.StatusCode.int2StatusCode
import akka.http.scaladsl.server.Directive.addByNameNullaryApply
import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.grpc.{CallOptions, ManagedChannelBuilder}
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.swagger.annotations._

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

object ClusterHttpManagementActor {

  val props = Props.apply(classOf[ClusterHttpManagementActor])
}

/**
  * A cluster singleton actor hosting Cluster Management/Administration HTTP server
  */
class ClusterHttpManagementActor extends Actor with ActorLogging  {
  override def receive: Receive = {case _ => {}}


  override def preStart(): Unit = {
    super.preStart()
    import scala.concurrent.duration._
    Try {
      Await.result(AkkaManagement(context.system).start(), 10.seconds)
    } match {
      case Success(_) => log.info(s"Started Akka Cluster Management HTTP server")
      case _ => throw new RuntimeException("Failed to start Akka Cluster Management HTTP server")
    }
  }



  override def postStop(): Unit = {
    super.preStart()
    import scala.concurrent.duration._
    Try {
      Await.result(AkkaManagement(context.system).stop(), 10.seconds)
    } match {
      case Success(_) => log.info(s"Stopped Akka Cluster Management HTTP server")
      case _ => throw new RuntimeException("Failed to stop Akka Cluster Management HTTP server")
    }
     super.postStop()
  }
}

final case class ClusterShardDetails(regions: Map[String, ShardDetails])

object ClusterHttpManagement extends ExtensionId[ClusterHttpManagement] with ExtensionIdProvider {
  override def lookup: ClusterHttpManagement.type = ClusterHttpManagement

  override def get(system: ActorSystem): ClusterHttpManagement = super.get(system)

  override def createExtension(system: ExtendedActorSystem): ClusterHttpManagement =
    new ClusterHttpManagement(system)

}

trait CorsSupport {

  //this directive adds access control headers to normal responses
  private def addAccessControlHeaders: Directive0 = {
    mapResponseHeaders { headers =>
      `Access-Control-Allow-Origin`.* +:
        `Access-Control-Allow-Credentials`(true) +:
        `Access-Control-Allow-Headers`("Authorization", "Content-Type", "Origin",
          "X-Atmosphere-tracking-id", "X-Atmosphere-Framework", "X-Cache-Date", "X-Atmosphere-Transport", "X-Requested-With", "*") +:
        headers
    }
  }

  //this handles preflight OPTIONS requests. TODO: see if can be done with rejection handler,
  //otherwise has to be under addAccessControlHeaders
  private def preflightRequestHandler: Route = options {
    complete(HttpResponse(200).withHeaders(
      `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)
    )
    )
  }

  def corsHandler(r: Route) = addAccessControlHeaders {
    preflightRequestHandler ~ r
  }

}

/**
  * Provides an HTTP management interface for [[akka.cluster.Cluster]].
  * Cluster management API implementation
  */
final class ClusterHttpManagement(system: ExtendedActorSystem) extends ManagementRouteProvider {

  private lazy val cluster = Cluster(system)

  val settings = new ClusterHttpManagementSettings(system.settings.config)

  /** Routes to be exposed by Akka cluster management */
  override def routes(routeProviderSettings: ManagementRouteProviderSettings): Route =
  // ignore the settings, don't carry any information these routes need
    new ClusterHttpManagementRoutesOverride(routeProviderSettings)(cluster)



  @Path("")
  @Api(value = "")
  class ClusterHttpManagementRoutesOverride(val settings: ManagementRouteProviderSettings) extends ClusterHttpManagementHelper with SwaggerHttpService with RouteConcatenation with CorsSupport{

    import akka.http.scaladsl.server.Directives._

    override val apiClasses:Set[Class[_]] = Set(this.getClass)
    override val host = s"${settings.selfBaseUri.authority.host.address}:${settings.selfBaseUri.authority.port}" //the url of your api, not swagger's json endpoint
    override val basePath = "/" //the basePath for the API you are exposing
    override val apiDocsPath = "api-docs" //where you want the swagger-json endpoint exposed
    override val info = Info() //provides license and other description details


    implicit val clusterShardDetailsFormat = jsonFormat1(ClusterShardDetails)

    implicit lazy val ec = AkkaCluster.miscEc

    @Path("/cluster/members")
    @ApiOperation(value = "Get cluster members info", notes = "", nickname = "", httpMethod = "GET", produces = "application/json")
    @ApiImplicitParams(Array(
    ))
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "Return Members", response = classOf[ClusterMembers])
    ))
    private def routeGetMembers(cluster: Cluster) =
      get {
        complete {
          val readView = ClusterReadViewAccess.internalReacView(cluster)
          val members = readView.state.members.map(memberToClusterMember)

          val unreachable = readView.reachability.observersGroupedByUnreachable.toSeq.sortBy(_._1).map {
            case (subject, observers) ⇒
              ClusterUnreachableMember(s"${subject.address}", observers.toSeq.sorted.map(m ⇒ s"${m.address}"))
          }

          val leader = readView.leader.map(_.toString)
          val oldest = cluster.state.members.toSeq
            .filter(node => node.status == MemberStatus.Up)
            .sorted(Member.ageOrdering)
            .headOption // we are only interested in the oldest one that is still Up
            .map(_.address.toString)

          ClusterMembers(s"${readView.selfAddress}", members, unreachable, leader, oldest)
        }
      }

    private def routePostMembers(cluster: Cluster) =
      post {
        formField('address) { addressString ⇒
          complete {
            val address = AddressFromURIString(addressString)
            cluster.join(address)
            ClusterHttpManagementMessage(s"Joining $address")
          }
        }
      }

    private def routeGetMember(cluster: Cluster, member: Member) =
      get {
        complete {
          memberToClusterMember(member)
        }
      }

    private def routeDeleteMember(cluster: Cluster, member: Member) =
      delete {
        complete {
          cluster.leave(member.uniqueAddress.address)
          ClusterHttpManagementMessage(s"Leaving ${member.uniqueAddress.address}")
        }
      }

    private def routePutMember(cluster: Cluster, member: Member) =
      put {
        formField('operation) { operation ⇒
          operation match {
            case "Down" ⇒
              cluster.down(member.uniqueAddress.address)
              complete(ClusterHttpManagementMessage(s"Downing ${member.uniqueAddress.address}"))
            case "Leave" ⇒
              cluster.leave(member.uniqueAddress.address)
              complete(ClusterHttpManagementMessage(s"Leaving ${member.uniqueAddress.address}"))
            case _ ⇒
              complete(StatusCodes.BadRequest → ClusterHttpManagementMessage("Operation not supported"))
          }
        }
      }

    private def findMember(cluster: Cluster, memberAddress: String): Option[Member] = {
      val readView = ClusterReadViewAccess.internalReacView(cluster)
      readView.members.find(
        m ⇒ s"${m.uniqueAddress.address}" == memberAddress || m.uniqueAddress.address.hostPort == memberAddress)
    }

    private def routesMember(cluster: Cluster) =
      path(Remaining) { memberAddress ⇒
        findMember(cluster, memberAddress) match {
          case Some(member) ⇒
            routeGetMember(cluster, member) ~ routeDeleteMember(cluster, member) ~ routePutMember(cluster, member)
          case None ⇒
            complete(StatusCodes.NotFound → ClusterHttpManagementMessage(s"Member [$memberAddress] not found"))
        }
      }

    @Path("/cluster/shards/{name}")
    @ApiOperation(value = "Get shard system info", notes = "", nickname = "", httpMethod = "GET", produces = "application/json")
    @ApiImplicitParams(Array(
      new ApiImplicitParam(name = "name", value = "Shard system name", required = true, dataType = "string", paramType = "path")
     ))
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "Return Entity", response = classOf[ClusterShardDetails]),
      new ApiResponse(code = 404, message = "Entity Not Found")
    ))
    private def routeGetShardInfo(cluster: Cluster, shardSystemName: String) =
      get {
        extractRequestContext { implicit ctx =>
          onComplete {
            import ctx.materializer
            ctx.request.discardEntityBytes().future
          } { case _ => onComplete {
            import scala.concurrent.duration._
            Patterns.ask(ClusterSharding(cluster.system)
              .shardRegion(shardSystemName), new ShardRegion.GetClusterShardingStats(5.seconds), 60.seconds)
              .mapTo[ShardRegion.ClusterShardingStats]
              .map { shardRegionStats =>
                ClusterShardDetails(shardRegionStats.regions.map(e => e._1.host.getOrElse("") ->
                  ShardDetails(e._2.stats.map(s => ShardRegionInfo(s._1, s._2)).toSeq)))
              }
          } {
            case Failure(_: AskTimeoutException) =>
              complete(StatusCodes.NotFound,
                s"Shard system $shardSystemName not responding, may have been terminated")
            case Failure(_: IllegalArgumentException) =>
              complete(StatusCodes.NotFound, s"Shard system $shardSystemName is not started")
            case Success(x) => complete(x)
          }
          }
        }
    }

    @Path("/cluster/shards")
    @ApiOperation(value = "Restart shard regions", notes = "", nickname = "", httpMethod = "POST", produces = "text/plain")
    @ApiImplicitParams(Array())
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "Restart confirmation"),
      new ApiResponse(code = 500, message = "Internal Server Error")
    ))
    private def routeRestartShardsRegion(cluster: Cluster) =
      post {
        extractRequestContext { implicit ctx =>
          onComplete {
            import ctx.materializer
            ctx.request.discardEntityBytes().future
          } { case _ => onComplete {
            val readView = ClusterReadViewAccess.internalReacView(cluster)
            val fmap = readView.state.members.map(m => restartShardingRegion(m.address.host.get).map(_ => m.address.host.getOrElse("")))
            Future.sequence(fmap)
          }{
            case Failure(e) => complete(StatusCodes.InternalServerError, s"Shard regions restart has failed: $e")
            case Success(x) => complete(StatusCodes.OK, s"Shard regions have been restarted on nodes ${x}")

          }}
          }
        }


    @Path("/cluster/grpc")
    @ApiOperation(value = "Run gRPC servers healthcheck", notes = "", nickname = "", httpMethod = "GET", produces = "text/plain")
    @ApiResponses(Array(
      new ApiResponse(code = 200, message = "Healthcheck status"),
      new ApiResponse(code = 500, message = "Internal Server Error")
    ))
    private def routeGetGrpcClusterHealthcheck(cluster: Cluster) =
      get {
        extractRequestContext { implicit ctx =>
          onComplete {
            import ctx.materializer
            ctx.request.discardEntityBytes().future
          }{case _ => onComplete {
            val readView = ClusterReadViewAccess.internalReacView(cluster)
            val fmap = readView.state.members.map(m => checkGrpcAlive(m.address.host.get))
            Future.sequence(fmap)
          }{
            case Failure(e) => complete(StatusCodes.InternalServerError, s"gRPC healthcheck failed: $e")
            case Success(s) => {
              val successes = s.filter(_.isLeft).map(_.left.get)
              val failures = s.filter(_.isRight).map(_.right.get)
              complete(StatusCodes.OK, s"gRPC healthcheck results: successes=$successes, failures=$failures")
            }
          }}
      }
    }



    private def checkGrpcAlive(hostname: String): Future[Either[String, String]] = {
      val channel = ManagedChannelBuilder
        .forAddress(hostname,AkkaCluster.configurationProvider.getConfig.getInt("grpc.port")
        ).asInstanceOf[ManagedChannelBuilder[NettyChannelBuilder]]
        .usePlaintext(true)
        .keepAliveTime(1, TimeUnit.MINUTES)
        .keepAliveWithoutCalls(true)
        .build
      val client = new ServerHealthcheckApiGrpc.ServerHealthcheckApiStub(channel, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS))
      client.ping(GrpcCheckAlive.defaultInstance).map(_ => Left(hostname)).recover{case _: Throwable => Right(hostname)}
        .map(x => {
          channel.shutdownNow()
          channel.awaitTermination(10, TimeUnit.SECONDS)
          x
        })
    }

  private def restartShardingRegion(hostname: String): Future[GrpcRestartShardingRegion] = {
    val channel = ManagedChannelBuilder
      .forAddress(hostname,AkkaCluster.configurationProvider.getConfig.getInt("grpc.port")
      ).asInstanceOf[ManagedChannelBuilder[NettyChannelBuilder]]
      .usePlaintext(true)
      .keepAliveTime(1, TimeUnit.MINUTES)
      .keepAliveWithoutCalls(true)
      .build
    val client = new ShardingRegionManagementApiGrpc.ShardingRegionManagementApiStub(channel, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS))
    client.restart(GrpcRestartShardingRegion.defaultInstance).map(x => {
      channel.shutdownNow()
      channel.awaitTermination(10, TimeUnit.SECONDS)
      x
    })

  }

    lazy val assets = pathPrefix("swagger") {
      getFromResourceDirectory("swagger") ~ pathSingleSlash(get(redirect("index.html", StatusCodes.PermanentRedirect)))
    }

    /**
      * Creates an instance of [[ClusterHttpManagementRoutes]] to manage the specified
      * [[akka.cluster.Cluster]] instance. This version does not provide Basic Authentication. It uses
      * the specified path `pathPrefixName`.
      */
    def apply(cluster: Cluster): Route =
      corsHandler(concat(
        pathPrefix("cluster" / "members") {
          concat(
            pathEndOrSingleSlash {
              routeGetMembers(cluster) ~ routePostMembers(cluster)
            },
            routesMember(cluster)
          )
        },
        pathPrefix("cluster" / "shards") {
          concat(
            pathEndOrSingleSlash {
              routeRestartShardsRegion(cluster)
            },
            path(Remaining) {shardSystemName ⇒ routeGetShardInfo(cluster, shardSystemName) }
          )
        },
        pathPrefix("cluster" / "grpc") {
          concat(
            pathEndOrSingleSlash {
              routeGetGrpcClusterHealthcheck(cluster)
            }
          )
        }
      )) ~ assets ~ this.routes
  }
}