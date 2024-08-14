package com.cachakka.streaming.akka;

import akka.actor.ActorSystem;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;


/**
 * This is a Flink plug-in that starts Akka co-lo within Flink JVM
 */
public class AkkaFauxMetricReporter implements MetricReporter{

    private static final Logger logger = LoggerFactory.getLogger(AkkaFauxMetricReporter.class);

    @Override
    public void open(MetricConfig config) {
        ActorSystem system = AkkaCluster.actorSystem();
        logger.info("Akka co-lo cluster system: " + system.toString() + " is running");
    }


    @Override
    public void close() {
        try {
            Await.result(AkkaCluster.actorSystem().terminate(), Duration.apply(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            logger.error("Failed to shutdown the Akka co-lo system");
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {}


    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {}
}
