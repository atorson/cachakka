package com.cachakka.streaming.metrics.utils;


import com.cachakka.streaming.metrics.calculator.*;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.metrics.MetricGroup;

import java.util.Arrays;
import java.util.List;
import java.util.Map;



public class MetricUtils {

    public static final String FLINK_METRIC_REPORTER_ROOT_PROPERTY = "metrics.reporter.graphite.root";

    /**  Metric values list state descriptor */
   public static final ListStateDescriptor<Map<List<String>, List<Long>>> METRIC_VALUES_STATE =
            new ListStateDescriptor<>("metric-values-list-state", new MapSerializer(
                    new ListSerializer(StringSerializer.INSTANCE), new ListSerializer(LongSerializer.INSTANCE)));

    /**
     * Basic metering package
     * @param <T> streaming data type
     * @return basic metering calculators package
     */
    public static <T> CompositeFlinkMetricsCalculator<T> basicMetricsPackage() {
        return new CompositeFlinkMetricsCalculator<T>()
                .withCalculator(new ResettingEventCounterCalculator<T>().withScope("EventCounter"))
                .withCalculator(new SimpleRateMeterCalculator<T>().withScope("RateMeter"))
                .withCalculator(new SimpleTrafficHistogramCalculator<T>().withScope("TrafficHistogram"));
    }

    /**
     * Basic metering package for sources (has restart counter)
     * @param <T>
     * @return
     */
    public static <T> CompositeFlinkMetricsCalculator<T> sourceMetricsPackage(){
        return MetricUtils.<T>basicMetricsPackage().withCalculator(new ResettingStartCounterCalculator<T>().withScope("RestartCounter"));
    }

    /**
     * Basic latency metrics calculator for function calls that can only be successful(single return code)
     * @param <T>
     * @return
     */
    public static <T> CompositeFlinkMetricsCalculator<T> latencyMetricsCalculator(){
        return new CompositeFlinkMetricsCalculator<T>().withCalculator(
                new VectorLatencyCalculator<>((in, out) -> Arrays.asList("LatencyHistogram", "SUCCESS")));

    }

    /**
     * Recursively adds a metric
     * @param parent parent group
     * @param scope child scope
     * @return child group
     */
    public static MetricGroup addAtScope(MetricGroup parent, List<String> scope){
        MetricGroup mg = parent;
        for (String s : scope) {
            mg = mg.addGroup(s);
        }
        return mg;
    }

}
