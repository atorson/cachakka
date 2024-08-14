package com.cachakka.streaming.metrics.core;

import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;

import java.util.List;

public interface MeterSpecificTrait extends MetricSpecificTrait<Meter>{

    @Override
    default Meter createMetric(List<String> scope) {
        return new DropwizardMeterWrapper(new com.codahale.metrics.Meter());
    }

    @Override
    default void actuateMetric(Meter meter, Long result) {
        meter.markEvent(result);
    }
}
