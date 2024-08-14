package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.CounterSpecificTrait;
import org.apache.flink.metrics.Counter;


/**
 * Scalar metric calculator measuring total accumulated event count since the start of the Flink job
 * @param <T>
 */
public final class SimpleEventCounterCalculator<T> extends ScalarFlinkMetricsCalculator<T,Void, Counter,SimpleEventCounterCalculator<T>> implements CounterSpecificTrait {}
