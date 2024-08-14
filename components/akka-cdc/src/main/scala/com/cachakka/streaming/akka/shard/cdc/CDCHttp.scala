package com.cachakka.streaming.akka.shard.cdc





import java.net.InetAddress

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive.addByNameNullaryApply
import akka.http.scaladsl.server._
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, Materializer}
import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import com.cachakka.streaming.akka.{AkkaCluster, CorsSupport, _}
import java.net.InetAddress

import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Success, Try}

object CDCHttpFacadeActor {

  val props = Props.apply(classOf[CDCHttpFacadeActor]).withDispatcher("dispatchers.affinity")

}

/**
  * Cluster singleton actor to host entity-specific Swagger API HTTP server
  */
class CDCHttpFacadeActor extends Actor with ActorLogging with CDCHttpRestfulServerModule {
  override def receive: Receive = {case _ => {}}

  private val config = AkkaCluster.configurationProvider.getConfig.getConfig("cdc")

  override implicit val actorSystem: ActorSystem = context.system
  override implicit val materializer: Materializer = ActorMaterializer()
  override implicit val ec: ExecutionContext = AkkaCluster.miscEc

  override val hostname: String = Try{config.getString("host")}.getOrElse(InetAddress.getLocalHost.getHostAddress)
  override val port: Int = config.getInt("port")


  import collection.JavaConverters._
  override val cdcRestModules: Set[CDCRestOperationsFacade] = config.getConfigList("entities").asScala
    .groupBy(_.getString("fqn")).keySet.map(findCompanion[CDCRestOperationsFacade](_, Thread.currentThread().getContextClassLoader).get)


  override val apiClasses:Set[Class[_]] = cdcRestModules.map(_.reflectRestApiDefinition)
  override val host = s"$hostname:$port" //the url of your api, not swagger's json endpoint
  override val basePath = "/" //the basePath for the API you are exposing
  override val apiDocsPath = "api-docs" //where you want the swagger-json endpoint exposed
  override val info = Info() //provides license and other description details

  override def preStart(): Unit = {
    super.preStart()
    if (start)
      log.info(s"Started CDC HTTP facade listening at $hostname:$port")
    else throw new RuntimeException (s"Failed to start CDC HTTP facade at $hostname:$port")

  }

  override def postStop(): Unit = {
    if (stop)
      log.info(s"Stopped CDC HTTP facade listening at $hostname:$port")
    else throw new RuntimeException (s"Failed to stop CDC HTTP facade running at $hostname:$port")
    super.postStop()
  }
}




trait CDCHttpRestfulServerModule extends SwaggerHttpService with RouteConcatenation with CorsSupport {

  implicit val actorSystem: ActorSystem
  implicit val materializer: Materializer
  implicit val ec: ExecutionContext


  val hostname: String
  val port: Int
  val cdcRestModules: Set[CDCRestOperationsFacade]

  protected var innerBinding: Option[ServerBinding] = None

  lazy val flow = Route.handlerFlow({
    // Swagger service for JSON-over-HTTP web services

    // JSON-over-HTTP service
    corsHandler(cdcRestModules.map(_.routes).reduce(_ ~ _)) ~ assets ~ this.routes
  })

  lazy val assets = pathPrefix("swagger") {
    getFromResourceDirectory("swagger") ~ pathSingleSlash(get(redirect("index.html", StatusCodes.PermanentRedirect)))
  }


  import scala.concurrent.duration._

  def isRunning = innerBinding.isDefined

  def start = if (!isRunning) this.synchronized{
    Try{
      Await.result(
        Http().bind(hostname, port).toMat(Sink.foreach(x => {
          x.flow.join(flow).run()})
        )(Keep.left).run()
        , 10.seconds)
    } match {
      case Success(x) => {
        innerBinding = Some(x)
        true
      }
      case _ => false
    }
  } else {false}


  def stop: Boolean = if (isRunning) this.synchronized{
    Try{
      Await.result(innerBinding.get.unbind(), 10.seconds)
    } match {
      case Success(_) => {
        innerBinding = None
        true
      }
      case _ => false
    }
  } else {false}

}



