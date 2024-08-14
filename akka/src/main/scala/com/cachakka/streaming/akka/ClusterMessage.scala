package com.cachakka.streaming.akka


import akka.actor.ActorRef
import com.trueaccord.scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}

import scala.concurrent.Future


trait ClusterMessage extends GeneratedMessage {}

trait ShardedEntityOperation {

  def getServiceName: String = ???

  def getServiceHandler(serviceActor: ActorRef): ShardedEntityHandler = ???
}

trait ShardedEntityOperationProvider {

  val operations: Set[ShardedEntityOperation]

  lazy val operationsMap: Map[Class[_<:ShardedEntityOperation], ShardedEntityOperation] = {
    val root = classOf[ShardedEntityOperation]
       this.operations
      .map(x => (x.getClass.getInterfaces.last -> x))
      .filter(c => root.isAssignableFrom(c._1))
      .map(c => (c._1.asInstanceOf[Class[_<:ShardedEntityOperation]], c._2)).toMap
  }

  def findInterfaceOperation[T<:ShardedEntityOperation](interface: Class[T]): Option[T] =
    operationsMap.get(interface).map(_.asInstanceOf[T])

  def findParentOperation[T<:ShardedEntityOperation](operationClass: Class[_<:T]): Option[T] =
    operationsMap.find(_._1.isAssignableFrom(operationClass)).map(_._2.asInstanceOf[T])

}

trait ShardedEntityOperationGeneratedEnumCompanion extends ShardedEntityOperationProvider {

  val values: Seq[ShardedEntityOperation]

  override val operations = values.toSet
}

trait UnknownShardedEntityOperation extends ShardedEntityOperation {

  private class NackServiceHandler(override protected val parentRef: ActorRef) extends BaseShardedEntityHandler{
    private val exc = new IllegalAccessException(s"Unknown operation for sharded service actor ${parentRef.path.toString}")
    override def innerHandle(entity: ShardedEntity) = Future.failed(exc)
  }

  override def getServiceName: String = "nack"

  override def getServiceHandler(serviceActor: ActorRef): ShardedEntityHandler = new NackServiceHandler(serviceActor)
}

case object UnknownShardedEntityOperation extends UnknownShardedEntityOperation

trait PassivateShardedEntityOperation extends ShardedEntityOperation

trait ShardedEntity extends ClusterMessage {

  def getShardID: String

  def setShardID(shardID: String): ShardedEntity

  def getOperation: ShardedEntityOperation = UnknownShardedEntityOperation

  def setOperation(operation: ShardedEntityOperation): ShardedEntity

  def getEntityID: String = getOperation.getServiceName
}

trait InstrumentalMessage extends ClusterMessage {

  def getInstrumentalCompanion: InstrumentalMessageCompanion = this.companion.asInstanceOf[InstrumentalMessageCompanion]

  def getMessageType: InstrumentalMessageType
}


trait InstrumentalMessageType


trait InstrumentalMessageCompanion{

  type M<:InstrumentalMessage

}

case object Ack extends InstrumentalMessageType {

  val ack = BaseAckMessage.defaultInstance
}

trait AckMessage extends InstrumentalMessage {
  override def getMessageType: InstrumentalMessageType = Ack
}

trait AckMessageCompanion extends InstrumentalMessageCompanion{

  override type M = AckMessage

}

case object Nack extends InstrumentalMessageType {

  val cmp = BaseNackMessage

  private val hollowMessage = cmp.defaultInstance

  def nackMessage(exc: Throwable) = hollowMessage.withExcString(exc.toString)

}

trait NackMessage extends InstrumentalMessage {

  override def getMessageType: InstrumentalMessageType = Nack

  def excString: String

  def withExcString(exc: String): NackMessage

  lazy val exc: Throwable = Option[String](excString).map(new RuntimeException(_)).orNull
}

trait NackMessageCompanion extends InstrumentalMessageCompanion {

    override type M = NackMessage

}
