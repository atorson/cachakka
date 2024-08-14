package com.cachakka.streaming.extensions

import com.datastax.driver.core.{BatchStatement, Cluster}
import io.getquill.context.ContextMacro
import io.getquill.context.cassandra.util.FutureConversions.toScalaFuture
import io.getquill.util.EnableReflectiveCalls
import io.getquill.{CassandraAsyncContext, SnakeCase}

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, Future}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.{Context => MacroContext}


class CqlBatchActionMacro(val c: MacroContext)
  extends ContextMacro {
  import c.universe.{Function => _, Ident => _, _}

  def prepareBatchAction(quoted: Tree): Tree =
    c.untypecheck {
      q"""
        ..${EnableReflectiveCalls(c)}
        val expanded = ${expand(extractAst(quoted))}
        ${c.prefix}.prepareBatchAction(
          expanded.string,
          expanded.prepare
        )
      """
    }
}

/**
  * Provides batch-API for Cassandra using Quill-IO
  * Note: it is using UNLOGGED mode (i.e. avoiding distributed log-journal overhead)
  * Note: Scala Macro black-magic is used. DO NOT MODIFY unless you're a real Scala wizard and Quill-IO expert
  * @param cluster
  * @param keyspace
  * @param preparedStatementCacheSize
  * @param ec
  */
class CQLBatchAwareAsyncContext(cluster: Cluster, keyspace: String, preparedStatementCacheSize: Long)(implicit val ec: ExecutionContext)
  extends CassandraAsyncContext[SnakeCase](SnakeCase, cluster, keyspace, preparedStatementCacheSize) {

  def prepareBatchAction(cql: String, prepare: Prepare)(implicit ec: ExecutionContext): Result[PrepareRow] =
    Future(prepare(this.prepare(cql))).map (_._2)

  def prepareBatchAction(quoted: Quoted[Action[_]]): Result[PrepareRow] = macro CqlBatchActionMacro.prepareBatchAction

  /**
    * Bulk query method. Executes multiple micro-batches of a given size, concurrently
    * @param transactions
    * @param batchSize
    * @param ec
    * @return
    */
  def executeBatchActions(transactions: Seq[PrepareRow], batchSize: Option[Int] = None)(implicit ec: ExecutionContext): Result[RunBatchActionResult] = batchSize match {
    case Some(b) => {
      val temp = transactions.foldLeft[(Seq[Seq[PrepareRow]], Seq[PrepareRow])]((Seq(), Seq()))((x,s) => {
        val y = x._2 :+ s
        if (y.size == b) {
          (x._1 :+ y, Seq())
        } else {
          (x._1, y)
        }
      })
      val fmap = (temp._2.isEmpty match {
        case false => temp._1 :+ temp._2
        case _ => temp._1
      }).map(l => innerExecuteBatchActions(l))
      Future.sequence(fmap).map(_ => {})
    }
    case _ => innerExecuteBatchActions(transactions)
  }

  protected def innerExecuteBatchActions(t: Seq[PrepareRow])(implicit ec: ExecutionContext): Result[RunBatchActionResult] = t.isEmpty match {
    case false => session.executeAsync(new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(JavaConverters.asJavaIterableConverter(t)
      .asJava)).map(_ => ())
    case true => Future.successful()
  }



}

