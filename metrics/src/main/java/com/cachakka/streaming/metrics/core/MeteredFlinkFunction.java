package com.cachakka.streaming.metrics.core;


import com.cachakka.streaming.core.api.StreamingFlowDataTypeAware;
import org.apache.flink.api.common.functions.RichFunction;

import java.util.function.Supplier;

/**
 * Metrics extension of Flink functions
 * Declares a richer interface around Flink functions: type aware (Flink leverages it) and metrics aware (so metrics can be retrieved from it)
 */
public interface MeteredFlinkFunction<T> extends RichFunction, StreamingFlowDataTypeAware<T>, Supplier<FlinkMetricCalculatorsRegistry>{

}
