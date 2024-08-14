package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.MetricSpecificTrait;
import com.cachakka.streaming.metrics.utils.MetricUtils;
import io.vavr.control.Try;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class LastProcessedTimeVectorMetricsCalculator<T, C extends LastProcessedTimeVectorMetricsCalculator<T,C>> extends VectorFlinkMetricsCalculator<T, Void, LastProcessedTimeGroupRankGauge, C> implements MetricSpecificTrait<LastProcessedTimeGroupRankGauge> {

    private static final Logger logger = LoggerFactory.getLogger(LastProcessedTimeVectorMetricsCalculator.class);

    @Override
    public LastProcessedTimeGroupRankGauge createMetric(List<String> scope) {
        return new LastProcessedTimeGroupRankGauge(scope.get(0), scope.get(1), scope.get(2), new AtomicReference<>());
    }

    @Override
    public void actuateMetric(LastProcessedTimeGroupRankGauge metric, Long result) {
       Long prev = metric.getLastTimeRef().get();
       metric.getLastTimeRef().set((prev != null && prev > result)? prev: result);
    }
}
