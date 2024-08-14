package com.cachakka.streaming.reporter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import io.vavr.Function1;
import org.apache.flink.dropwizard.metrics.*;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.SortedMap;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;

public abstract class FilteredScheduledDropwizardReporter implements MetricReporter, Scheduled, Reporter, CharacterFilter{

    private static final Logger logger = LoggerFactory.getLogger(FilteredScheduledDropwizardReporter.class);
    protected Function1<String, Optional<String>> filter;
    protected final MetricRegistry registry;
    protected ScheduledReporter reporter;


    protected FilteredScheduledDropwizardReporter(){
        this.filter = s -> Optional.of(s);
        this.registry = new MetricRegistry();
    }

    protected FilteredScheduledDropwizardReporter withBasicFilter(@Nonnull final String root){
        return this.withFilter(s -> s.contains(root)? Optional.of(s): Optional.empty());
    }

    protected FilteredScheduledDropwizardReporter withFilter(final Function1<String,Optional<String>> filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public void open(MetricConfig config) {
        try {
            this.reporter = getReporter(config);
        } catch (Exception e) {
            logger.error("Failed to initialize primary metrics reporter from %", config);
            logger.info("Falling back on the SLF4J metrics reporter");
            this.reporter = Slf4jReporter
                    .forRegistry(this.registry)
                    .outputTo(logger)
                    .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO)
                    .build();
        }
        String root = config.getProperty("root");
        if (root != null){
            withFilter(s -> (s.contains(root) || s.contains("numberOfFailedCheckpoints"))? Optional.of(s): Optional.empty());
        }
    }

    @Override
    public void close() {
        this.reporter.stop();
    }

    protected Optional<String> getFullName(String metricName, MetricGroup group){
        return filter.apply(group.getMetricIdentifier(metricName, this));
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            getFullName(metricName, group).ifPresent(fullName ->
            Match(metric).of(
                    Case($(instanceOf(Counter.class)), c -> this.registry.register(fullName, new FlinkCounterWrapper(c))),
                    Case($(instanceOf(DropwizardMeterWrapper.class)), m -> this.registry.register(fullName, m.getDropwizardMeter())),
                    Case($(instanceOf(Meter.class)), m -> this.registry.register(fullName, new FlinkMeterWrapper(m))),
                    Case($(instanceOf(DropwizardHistogramWrapper.class)), h -> this.registry.register(fullName, h.getDropwizardHistogram())),
                    Case($(instanceOf(Histogram.class)), h -> this.registry.register(fullName, new FlinkHistogramWrapper(h))),
                    Case($(instanceOf(Gauge.class)), g -> this.registry.register(fullName, FlinkGaugeWrapper.fromGauge(g))),
                    Case($(), (unsupported) -> {
                        throw new UnsupportedOperationException(String.format("Flink metric %s is not supported", unsupported));
                    })
            ));
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            getFullName(metricName, group).ifPresent(fullName -> this.registry.remove(fullName));
        }
    }

    @Override
    public String filterCharacters(String metricName) {
        char[] chars = null;
        final int strLen = metricName.length();
        int pos = 0;

        for (int i = 0; i < strLen; i++) {
            final char c = metricName.charAt(i);
            switch (c) {
                case '.':
                    if (chars == null) {
                        chars = metricName.toCharArray();
                    }
                    chars[pos++] = '-';
                    break;
                case '"':
                    if (chars == null) {
                        chars = metricName.toCharArray();
                    }
                    break;

                default:
                    if (chars != null) {
                        chars[pos] = c;
                    }
                    pos++;
            }
        }

        return chars == null ? metricName : new String(chars, 0, pos);
    }

    @Override
    public void report() {
        // we do not need to lock here, because the dropwizard registry is
        // internally a concurrent map
        @SuppressWarnings("rawtypes")
        final SortedMap<String, com.codahale.metrics.Gauge> gauges = this.registry.getGauges();
        final SortedMap<String, com.codahale.metrics.Counter> counters = this.registry.getCounters();
        final SortedMap<String, com.codahale.metrics.Histogram> histograms = this.registry.getHistograms();
        final SortedMap<String, com.codahale.metrics.Meter> meters = this.registry.getMeters();
        final SortedMap<String, com.codahale.metrics.Timer> timers = this.registry.getTimers();
        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()){
           logger.debug(String.format("No metrics to report from %s", this));
        } else {
            logger.debug(String.format("Reporting %s meters, %s counters, %s gauges, %s histograms, %s timers from %s", meters.size(), counters.size(), gauges.size(),histograms.size(), timers.size(), this));
            this.reporter.report(gauges, counters, histograms, meters, timers);
        }
    }

    public abstract ScheduledReporter getReporter(MetricConfig config) throws Exception;
}
