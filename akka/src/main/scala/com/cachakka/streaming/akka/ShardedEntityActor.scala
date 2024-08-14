package com.cachakka.streaming.akka

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, ReceiveTimeout, Terminated}
import akka.cluster.sharding.ShardRegion
import com.google.common.base.Strings
import com.google.common.reflect.TypeToken

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


object ShardingFunction {

  private case class JavaShardingFunction[T](asJava: java.util.function.Function[T,String]) extends ShardingFunction[T]{
    override def apply(v: T): String = asJava.apply(v)
  }

  private case class ScalaShardingFunction[T](asScala: T => String) extends ShardingFunction[T]{
    override def apply(v: T): String = asScala.apply(v)
  }

  def asJava[T](f: java.util.function.Function[T,String]): ShardingFunction[T] = JavaShardingFunction[T](f)

  def asScala[T](f: T => String): ShardingFunction[T] = ScalaShardingFunction[T](f)


}

/**
  * Function used to automatically compute consistent shardIds
  * @tparam T
  */
trait ShardingFunction[T]{

  def apply(v: T): String
}

/**
  * Decoration to put on entities which allow to auto-compute shard IDs based on a particular set of fields
  * @tparam E
  */
trait AutoShardedEntity[E<:ShardedEntity]{

  self: E =>

  def processShardId(operation: ShardedEntityOperation = UnknownShardedEntityOperation): E

}

object ShardedEntityActor {

  val props = Props.apply(classOf[ShardedEntityActor]).withDispatcher("dispatchers.sharding")

}

case class ShardedEntityActorWatch(ref: ActorRef)

case class ShardedEntityActorOnStop(callback: () => Unit)

/**
  * Mesage used to trigger a certain follow-up operation on an actor from within another message that is being executed by this actor
  */
trait ShardedEntityActorDispatchAction {

  type T

  val typeToken: TypeToken[T]

  def execute(prev: Option[Try[T]]): Future[T]

}

/**
  * Multi-threaded high-throuput version of the sharded actor logic
  * Handler methods are executed outside of the actor dispatched thread, in parallel
  * This way, sharded actor threads are never blocked and can process vast throughputs
  */
trait ShardedEntityHandler {

  def handle(entity: ShardedEntity): Future[Option[ClusterMessage]]

}

trait BaseShardedEntityHandler extends ShardedEntityHandler {

  protected val parentRef: ActorRef

  override def handle(entity: ShardedEntity): Future[Option[ClusterMessage]] = entity.getOperation match {
    case _: PassivateShardedEntityOperation => {
      parentRef ! ReceiveTimeout
      entity match {
        case v: AsyncAskMessage[_] => Future.successful(Some(Ack.ack.withCorrelationId(v.correlationId)))
        case _ => Future.successful(Some(Ack.ack))
      }
    }

    case _ => innerHandle(entity)
  }

  protected  def innerHandle(entity: ShardedEntity): Future[Option[ClusterMessage]]

}

/**
  * Uniform implementation of the sharding services actor
  * Logic is the same for any sharded service (handlers are different)
  * Instances of this actor are created automatically by Akka based on unique shardID+entityID combinations
  */
class ShardedEntityActor extends Actor with ActorLogging{

  import scala.collection.JavaConverters._

  implicit lazy val ec = AkkaCluster.actorSystem.dispatchers.lookup("dispatchers.sharding")

  var handler: ShardedEntityHandler = null
  var terminate: Option[()=>Unit] = None
  val dispatchActionStateMap = TrieMap[TypeToken[_], Try[_]]()

  override def preStart(): Unit = {
    import scala.concurrent.duration._
    super.preStart()
    context.setReceiveTimeout(AkkaCluster.SHARD_RECEIVE_TIMEOUT_SEC.seconds)
    log.info(s"Started a sharded entity  actor at ${this.context.self.path}")
  }

  override def postStop(): Unit = {
    log.info(s"Stopped a sharded entity  actor at ${this.context.self.path}")
    terminate.foreach(_.apply())
    super.postStop()
  }

  private def getHandler[E<:ShardedEntity](m: ShardedEntity): ShardedEntityHandler = if (handler == null) {
     handler = m.getOperation.getServiceHandler(context.self)
     handler
  } else {handler}

  override def receive: Receive = {

    /**
      * Dispatch action handling. Note that this functionality is much more limited than sharding entity and serves a narrow use-case
      * of invoking follow-up actions from within a sharded message being handled
      */
    case a: ShardedEntityActorDispatchAction => {
      val recipient = sender()
      val correlationIdOption: Option[String] = a match {
        case v: AsyncAskMessage[_] => Some(v.correlationId)
        case _ => None
      }
      a.execute(dispatchActionStateMap.get(a.typeToken).map(_.asInstanceOf[Try[a.T]])).onComplete(r => {
        dispatchActionStateMap.put(a.typeToken,r)
        if (recipient != null && !recipient.path.toStringWithoutAddress.contains("deadLetters")) {
          val m = r match {
            case Failure(exc) => Nack.nackMessage(exc)
            case _ => Ack.ack
          }
          correlationIdOption match {
            case Some(id) => recipient ! m.withCorrelationId(id)
            case _ => recipient ! m
          }
        }
      })

    }

    /**
      * Sharded service logic: invoke handler and deliver the response back to the sender
      */
    case m: ShardedEntity => {
      val handler = getHandler(m)
      log.debug(s"Handling sharded entity $m with handler ${handler.getClass}...")
      val recipient = sender()
      val correlationIdOption: Option[String] = m match {
        case v: AsyncAskMessage[_] => Some(v.correlationId)
        case _ => None
      }
      val shardId = m.getShardID
      handler.handle(m).onComplete(_ match {
        case Failure(exc) => {
          val n = correlationIdOption match {
            case Some(id) => Nack.nackMessage(exc).withCorrelationId(id)
            case _ => Nack.nackMessage(exc)
          }
          recipient ! n
        }
        case Success(Some(v)) => v match{
          case a: ShardedEntity with AsyncAskMessage[_] if Strings.isNullOrEmpty(a.correlationId) => {
            log.debug(s"Sharding services actor for shard $shardId is returning entity of type ${a.getClass.getSimpleName} to the recipient ${recipient.path}")
            recipient ! v
          }
          case _ => recipient ! v
        }

        case _ => {}
      })
    }

    case ShardedEntityActorWatch(ref) => context.watch(ref)
    case ShardedEntityActorOnStop(callback) => terminate = Some(callback)
    case ReceiveTimeout => passivate()
    case Terminated(_) => passivate()
  }

  /**
    * Graceful passivation logic (triggered by either death-watch/Terminated or explicitly by idling/ReceiveTimeout signals
    * Note: there is also a gRPC service for invoking Passivate by 3rd party code/users
    */
  private def passivate(): Unit = {
    log.info(s"Passivating shard ${context.parent}")
    context.parent.tell(new ShardRegion.Passivate(PoisonPill.getInstance), context.self)
  }

}
