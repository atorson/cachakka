package com.cachakka.streaming.reporter


import java.util.Date

import com.codahale.metrics.Reporter
import com.google.common.base.CharMatcher
import com.typesafe.scalalogging.LazyLogging
import com.cachakka.streaming.core.utils.Utils
import com.cachakka.streaming.extensions.CQLBatchAwareAsyncContext
import org.apache.flink.metrics.reporter.{MetricReporter, Scheduled}
import org.apache.flink.metrics._

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.concurrent.{Await, Future}


case class GroupRankMetric(val shard: String, val updatedAt: Date, groupRankName: String, val groupMemberId: String, val groupMemberName: String,
 val fullMetricId: String, val metricValue: Double)

/**
  * Metric reporter that maintains mutable rank values for a grouped family of metrics
  */
trait CassandraGroupRankingMetricsReporter extends MetricReporter with Scheduled with Reporter with LazyLogging{

  protected def cassContext: CQLBatchAwareAsyncContext

  protected val registry = TrieMap[String, com.cachakka.streaming.metrics.core.GroupRankMetric]()

  private val SCOPE_SEPARATOR: Char = '.'

  private val USER_SCOPE_PREFIX = "zzzzzzz"

  private val REPORTING_BATCH_SIZE = 500

  private val nameDimensions = Set("job_name","operator_name","subtask_index")

  private var root: String = null

  override def open(config: MetricConfig): Unit = {
    root = config.getString("root", "sp")
  }

  override def close(): Unit = {}

  override def notifyOfAddedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = metric match {
    case rm: com.cachakka.streaming.metrics.core.GroupRankMetric => registry.putIfAbsent(getMetricId(metricName,group), rm)
    case _ => {}
  }

  override def notifyOfRemovedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = {
    registry.remove(getMetricId(metricName,group))
  }

  override def report(): Unit = { // we do not need to lock here, because the dropwizard registry is
    val ctx = cassContext
    import ctx._
    def reportToCass(rankValues: Seq[GroupRankMetric]): Future[Unit] = {
      try {
        val fs = rankValues
          .map(r => quote {
            query[GroupRankMetric].insert(lift(r))
          }).map(ctx.prepareBatchAction(_))
        Future.sequence(fs).map(ctx.executeBatchActions(_, Some(200)))
      } catch {
        case exc: Throwable => Future.failed(exc)
      }
    }
    val now =  Utils.currentTime()
    val temp = registry.toSeq.foldLeft[(Seq[Seq[(String, com.cachakka.streaming.metrics.core.GroupRankMetric)]],
      Seq[(String,com.cachakka.streaming.metrics.core.GroupRankMetric)])]((Seq(), Seq()))((x,s) => {
      val y  = x._2.:+[(String,com.cachakka.streaming.metrics.core.GroupRankMetric),
        Seq[(String,com.cachakka.streaming.metrics.core.GroupRankMetric)]](s)
      if (y.size == REPORTING_BATCH_SIZE) {
        (x._1.:+[Seq[(String,com.cachakka.streaming.metrics.core.GroupRankMetric)],
          Seq[Seq[(String, com.cachakka.streaming.metrics.core.GroupRankMetric)]]](y), Seq())
      } else {
        (x._1, y)
      }
    })
    val f = (temp._1 :+ temp._2).foldLeft[Future[Unit]](Future.successful())((y,l) => y.flatMap[Unit](_ =>
      reportToCass(l.map(e => GroupRankMetric(now.getYear.toString, now.toDate, e._2.getGroupRankName, e._2.getGroupMemberId, e._2.getGroupMemberName, e._1, e._2 match {
        case c: Counter => c.getCount.toDouble
        case m: Meter => m.getRate
        case h: Histogram => h.getStatistics.getMean
        case g: Gauge[_] => g.getValue.toString.toDouble
        case _ => 0.0
      })))
    ))
    f.onSuccess{case _ => logger.debug(s"Reported ${registry.size} metrics to Cass, latency: ${System.currentTimeMillis() - now.getMillis}")}
    f.onFailure{case x: Throwable => logger.error(s"Failed to report ${registry.size} metrics to Cass", x)}
  }



  private def getMetricId(metricName: String, group: MetricGroup) = {
    val scope = group.getScopeComponents.toSeq :+ metricName
    val filteredScope = scope.drop(scope.indexOf(root)).zipWithIndex.map(e => s"$USER_SCOPE_PREFIX${e._2}" -> e._1)
    import scala.collection.JavaConverters._
    val truncatedName = (SortedMap[String,String]() ++ group.getAllVariables.asScala ++ filteredScope).foldLeft[String]("")((x, e) => {
      val keyRaw = e._1
      val key = keyRaw.substring(1, keyRaw.length - 1)
      val value = e._2
      (scope.contains(value), nameDimensions.contains(key), keyRaw.contains(USER_SCOPE_PREFIX)) match {
        case (true, true, _) | (_, _, true) => s"$x$value$SCOPE_SEPARATOR"
        case _ => x
      }
    })
    CharMatcher.is(SCOPE_SEPARATOR).trimTrailingFrom(truncatedName)
  }

}
