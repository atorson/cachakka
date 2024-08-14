package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import com.cachakka.streaming.metrics.core.FlinkMetricsCalculator;
import com.cachakka.streaming.metrics.utils.MetricUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.*;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;

/**
 * Parent of the metric calculators hierarchy. Every implementation should leverage it (unless there is a strong reason to provide a custom 'bootstrap' behavior)
 * @param <T> streaming data type
 * @param <C> calculator sub-type (needed to implement a strongly-typed builder)
 */
abstract public class AbstractFlinkMetricsCalculator<T, C extends AbstractFlinkMetricsCalculator<T,C>> implements FlinkMetricsCalculator<T> {

    protected TypeInformation<T> streamingDataType;

    protected AbstractFlinkMetricsCalculator(){
        final FlinkReflectionUtils.ReflectionTypeExtractor<T> blank = new FlinkReflectionUtils.ReflectionTypeExtractor<>();
        streamingDataType = blank.extractTypeInformation(this, AbstractFlinkMetricsCalculator.class,0)
                .getOrElse((TypeInformation<T>) null);
    }

    @Override
    public TypeInformation<T> getStreamingDataType() {
        return streamingDataType;
    }

    public synchronized C withTypeInfo(@Nonnull TypeInformation<T> typeInfo){
        if (streamingDataType != null) return (C) this;
        streamingDataType = typeInfo;
        return (C) this;
    }

    @Override
    public C with(@Nonnull TypeInformation<T> typeInfo) {
        return withTypeInfo(typeInfo);
    }

    @Override
    public void bootstrap(@Nonnull RuntimeContext ctx) {
        //just initial static metrics registration
        registerMetrics(get(),ctx);
    }

    protected static void registerMetrics(Map<List<String>, Metric> metrics, RuntimeContext ctx){
        metrics.forEach((scope, metric) -> {
            // add name-scope
            final String name = !scope.isEmpty() ? scope.get(scope.size() - 1) : (metric.getClass() + "-" + UUID.randomUUID());
            final List<String> nameScope = !scope.isEmpty() ? scope.subList(0, scope.size() - 1) : Collections.emptyList();
            final MetricGroup metricGroup = MetricUtils.addAtScope(ctx.getMetricGroup(), nameScope);
            Match(metric).of(
                    Case($(instanceOf(Meter.class)), meter -> metricGroup.meter(name, meter)),
                    Case($(instanceOf(Counter.class)), counter -> metricGroup.counter(name, counter)),
                    Case($(instanceOf(Histogram.class)), histogram -> metricGroup.histogram(name, histogram)),
                    Case($(instanceOf(Gauge.class)), gauge -> metricGroup.gauge(name, gauge)),
                    Case($(), (unsupported) -> {
                        throw new UnsupportedOperationException(String.format("Flink metric %s is not supported", unsupported));
                    }));
        });
    }
}
