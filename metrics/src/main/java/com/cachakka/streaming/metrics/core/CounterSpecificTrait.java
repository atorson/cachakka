package com.cachakka.streaming.metrics.core;

import io.vavr.control.Try;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;

import java.util.List;

public interface CounterSpecificTrait extends MetricSpecificTrait<Counter> {

    @Override
    default Counter createMetric(List<String> scope) {
        return new SimpleCounter();
    }

    @Override
    default void actuateMetric(Counter metric, Long result) {
       Try<Void> r = result != null? result > 0? Try.run(() -> metric.inc(result)): Try.run(() -> metric.dec(-1*result)): Try.success(null);
    }
}
