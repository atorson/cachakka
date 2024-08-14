package com.cachakka.streaming.metrics.core;

import org.joda.time.DateTime;

import java.util.Optional;

/**
 * Represents a rich wrapper around each piece of streaming data flowing through Flink functions
 * Wrapping includes context used by Flink metrics calculators
 * @param <T> type of streaming data
 * @param <C> concrete type of the invokation context class
 */
public interface FlinkMetricsInvokationContext<T, C extends FlinkMetricsInvokationContext<T,C>> {

    DateTime getInvokeTime();

    /**
     * Only empty in case output context is invoked before output is available
     * @return option
     */
    Optional<T> getData();

    /**
     * Gets a concrete-typed view of self
     * Allows metric calculators (they accept() it) to change their behavior based on sub-types of FlinkMetricsInvokationContext
     * This provides further insight & de-encapsulation of the data to moderate metrics' behavior on
     * Example: output metrics may extract scope from input (from context that holds it) even though output data type may type may lack means for it
     * Example: latency metrics functionality heavily leverages this sub-typing
     * @return
     */
    C getReifiedSelf();
}
