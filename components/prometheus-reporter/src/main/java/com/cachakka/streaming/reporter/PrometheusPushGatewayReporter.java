package com.cachakka.streaming.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

    private PushGateway pushGateway;
    private String jobName;

    @Override
    public void open(MetricConfig config) {
        super.open(config);

        String host = config.getString("host", "localhost");
        int port = config.getInteger("port", 9091);
        jobName = "flink_prometheus_reporter";

        if (host == null || host.isEmpty() || port < 1) {
            throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        pushGateway = new PushGateway(host + ':' + port);

        log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName: {}}", host, port, jobName);
    }


    @Override
    public void report() {
        try {
            pushGateway.pushAdd(CollectorRegistry.defaultRegistry, jobName);
        } catch (Exception e) {
            log.warn("Failed to push metrics to PushGateway with jobName {}.", jobName, e);
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
