package com.cachakka.streaming.akka.shard.cdc


import akka.http.scaladsl.server.{Route, RouteConcatenation}
import com.trueaccord.scalapb.GeneratedMessageCompanion
import com.cachakka.streaming.akka.ShardedEntity
import com.cachakka.streaming.akka.shard.cdc.CDCShardExtension.CDC
import org.joda.time.DateTime

import scala.concurrent.Future
import scalapb.descriptors.FieldDescriptor


object NaturalKeyFunction {

  private case class JavaNaturalKeyFunction[T](asJava: java.util.function.Function[T,String]) extends NaturalKeyFunction[T]{
    override def apply(v: T): String = asJava.apply(v)
  }

  private case class ScalaNaturalKeyFunction[T](asScala: T => String) extends NaturalKeyFunction[T]{
    override def apply(v: T): String = asScala.apply(v)
  }

  def asJava[T](f: java.util.function.Function[T,String]): NaturalKeyFunction[T] = JavaNaturalKeyFunction[T](f)

  def asScala[T](f: T => String): NaturalKeyFunction[T] = ScalaNaturalKeyFunction[T](f)

}

trait NaturalKeyFunction[T]{

  def apply(v:T): String
}


trait AutoKeyedEntity[E<:CDCEntity]{

  self: E =>

  def processNaturalKey: E

}

/**
  * Parent interface for any enums that may be used in CDC Entities
  * Enables Cassandra and Protobuf SerDe support
  */
trait CDCEnum {

  def name: String

}

trait CDCAgent extends CDCEnum

trait CDCApiReturnCode extends CDCEnum

trait CDCApiCodeSuccess extends CDCApiReturnCode

trait CDCApiName extends CDCEnum {
  def externalApiName: String = ???
}

trait CDCLifecycleStatus extends CDCEnum {
  def isActive: Boolean = false
}

trait CDCActiveStatus extends CDCLifecycleStatus {
  override def isActive = true
}

trait CDCInactiveStatus extends CDCLifecycleStatus {
  override def isActive = false
}

case object UnspecifiedAgent extends CDCAgent {
  override val name: String = "UNSPECIFIED_AGENT"
}

case object Unspecified extends CDCEnum {

  override val name: String = "UNSPECIFIED"

}


object CDCEnumsProvider {

  @annotation.varargs
  def apply(processors: CDCEnumsProvider *): CDCEnumsProvider = new DefaultCDCEnumsProvider(processors)

  private class DefaultCDCEnumsProvider(override val enums: Set[CDCEnum]) extends CDCEnumsProvider{
    def this(providers: Seq[CDCEnumsProvider]) = this(providers.foldLeft[Set[CDCEnum]](Set())((x,p) => x ++ p.enums))
  }

}

/**
  * This provider is injected by Guice and used by Quill-IO to deserialize enums from Cassandra strings
  */
trait CDCEnumsProvider {

  val enums: Set[CDCEnum]

}

trait CDCGeneratedEnumsProvider extends CDCEnumsProvider{

  val values: Seq[CDCEnum]

  override val enums = values.toSet
}


/**
  * Limited-memory audit-trail kept for every CDCEntity
  * It is a map of timestamp where keys are CDCAgents (anything that can modify a CDCEntity and has a name)
  */
trait CDCEntityUpdateTrail {

  def lastUpdatedBy(agent: CDCAgent): Option[DateTime]

  def updatesSince(time: DateTime): Map[CDCAgent, DateTime]

  def lastUpdated: DateTime

  def lastUpdater: CDCAgent

  def updaters: Seq[CDCAgent]

}

/**
  * Key interface for all CDC and Cache sharded service operations
  * Note the companion's API
  */
trait CDCEntity extends ShardedEntity with CDCEntityUpdateTrail {

  type CDE <:CDC[CDE]

  def reify: CDE = this.asInstanceOf[CDE]

  def cdcCompanion: CDCEntityCompanion[CDE]

  def markUpdate(updater: CDCAgent): CDE

  def markUpdate(updater: CDCAgent, updated: DateTime): CDE

  def getKey: String

  def setKey(key: String): CDE
}

/**
  * This utility interface allows to wrap/unwrap collections of CDCEntities in bulk invokations of sharded services
  * @tparam E
  */
trait CDCEntityResolver[E<:ShardedEntity]{

  type DT = E

  type CD<:CDCEntity

  def resolveCDCEntities(entity: DT): Seq[CD]

  def wrapCDCEntities(entities: Seq[CD], defaultWrapper: Option[DT] = None): DT

}

object CDCProcessorProvider {

  def apply[E<:CDCEntity](processors: CDCEntityProcessor[E]*): CDCProcessorProvider[E] = DefaultCDCProcessorProvider[E](processors)

  private case class DefaultCDCProcessorProvider[E<:CDCEntity](override val processors: Iterable[CDCEntityProcessor[E]]) extends CDCProcessorProvider[E]
}

/**
  * Interceptor trait of Change-Data-Capture stream flowing out of Cassandra
  * See the implementations
  * New implementations can be added via Guice injection
  * @tparam E
  */
trait CDCEntityProcessor[E<:CDCEntity] {

  def processCDCEntities(entities: Iterable[E]): Future[Unit]

}

trait CDCProcessorProvider[E<:CDCEntity] {

  val processors: Iterable[CDCEntityProcessor[E]]

}

/**
  * Trait to encapsulate all DB operation queries
  * See the underlying Quill-IO implementations
  * @tparam E
  */
trait CDCDbOperationsFacade[E<:CDCEntity] {

  def upsertEntities(entities: List[E]): Future[Unit]

  def queryEntities(shardID: String, keys: Option[Set[String]] = None, limit: Option[Int] = None): Future[List[E]]

}

/**
  * Trait to encapsulate all Swagger API implementations (may vary per entity - and definitely carry entity-specific Swagger annotations)
  * See the underlying Quill-IO implementations
  */
trait CDCRestOperationsFacade extends RouteConcatenation{

  def reflectRestApiDefinition: Class[_<:CDCRestOperationsFacade]

  def PUT: Route

  def GET: Route

  def DB: Route

  def QUERY: Route

  def WARMUP: Route

  def CLEANUP: Route

  def SHARDS: Route

  def CONSUMERS: Route

  def ASSIGNMENTS: Route

  lazy val routes: Route = PUT ~ GET ~ DB ~ QUERY ~ WARMUP ~ CLEANUP ~ SHARDS ~ CONSUMERS ~ ASSIGNMENTS
}



trait CDCEntityCompanion[E<:CDC[E]] extends GeneratedMessageCompanion[E] with CDCProcessorProvider[E] with CDCDbOperationsFacade[E] with CDCEnumsProvider with CDCRestOperationsFacade {
  val clazz: Class[E]
}