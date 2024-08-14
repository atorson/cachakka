package com.cachakka.streaming.reporter;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteUDP;
import com.codahale.metrics.graphite.PickledGraphite;
import org.apache.flink.metrics.MetricConfig;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;


public class GraphiteMetricReporter extends FilteredScheduledDropwizardReporter {

    @Override
    public ScheduledReporter getReporter(MetricConfig config) throws Exception{
        String host = config.getString("host", "localhost");
        Integer port = config.getInteger("port", 2004);
        GraphiteReporter.Builder b = GraphiteReporter.forRegistry(this.registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL);
        return b.build(new PickledGraphite(new InetSocketAddress(host, port)));
    }
}
