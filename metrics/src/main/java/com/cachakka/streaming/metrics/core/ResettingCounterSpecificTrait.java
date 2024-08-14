package com.cachakka.streaming.metrics.core;


import io.vavr.control.Try;

import java.util.List;

public interface ResettingCounterSpecificTrait extends MetricSpecificTrait<ResettingCounter> {

    @Override
    default ResettingCounter createMetric(List<String> scope) {
        return new ResettingCounter(getResetIntervalDays());
    }

    @Override
    default void actuateMetric(ResettingCounter metric, Long result) {
        Try<Void> r = result != null? result >= 0? Try.run(() -> metric.inc(result)): Try.run(() -> metric.dec(-1*result)): Try.success(null);
    }

    /**
     * Returns reset interval (1 day, by default)
     * Reset happens at midnight
     * @return interval in days
     */
    default int getResetIntervalDays(){
        return 1;
    }

}
