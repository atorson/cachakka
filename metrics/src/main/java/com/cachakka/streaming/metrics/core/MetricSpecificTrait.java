package com.cachakka.streaming.metrics.core;

import org.apache.flink.metrics.Metric;

import java.util.List;

/**
 * This trait encapsulates re-usable specific methods for each type of Flink metric
 * Specific methods include: factory create() method; metric update method etc.
 * Children should be mixed-in to metric calculators
 * @param <M> sub-type of metric (counter, meter, etc.)
 */
public interface MetricSpecificTrait<M extends Metric> {

    /**
     * Children must override this method so that specific meter instances can be constructed
     * @param scope scope of this metric
     * @return new meter instance
     */
    M createMetric(List<String> scope);

    /**
     * Children must override this method so that meters can be updated with scalar long results (works for all Flink metric types)
     * @param result metric update
     */
    void actuateMetric(M metric, Long result);
}
