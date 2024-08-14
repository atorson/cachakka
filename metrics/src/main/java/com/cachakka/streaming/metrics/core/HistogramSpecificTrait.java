package com.cachakka.streaming.metrics.core;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;

import java.util.List;

public interface HistogramSpecificTrait extends MetricSpecificTrait<Histogram>{

    @Override
    default Histogram createMetric(List<String> scope) {
        return new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir()));
    }


    @Override
    default void actuateMetric(Histogram meter, Long result) { meter.update(result);}
}
