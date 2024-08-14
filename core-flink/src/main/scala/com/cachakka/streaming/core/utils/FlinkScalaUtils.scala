package com.cachakka.streaming.core.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.async.ResultFuture

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by kshah2 on 7/27/18.
  */
trait FlinkScalaUtils extends LazyLogging{

  def scalaToFlinkFutureFuse[T,U](scalaFuture: Future[T], flinkFuture: ResultFuture[U], mapper: java.util.function.Function[T,U])(implicit ec: ExecutionContext): Unit =
    scalaFuture.onComplete[Unit](t => t match {
      case Success(x) => Try(mapper.apply(x)) match {
        case Success(u: U) => flinkFuture.complete(java.util.Collections.singletonList(u))
        case Failure(exc) =>  flinkFuture.completeExceptionally(exc)
      }
      case Failure(e) => flinkFuture.completeExceptionally(e)
    })

}

object FlinkScalaUtils extends FlinkScalaUtils
