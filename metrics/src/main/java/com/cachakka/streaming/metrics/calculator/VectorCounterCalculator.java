package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.CounterSpecificTrait;
import org.apache.flink.metrics.Counter;

public final class VectorCounterCalculator<T> extends VectorFlinkMetricsCalculator<T, Void, Counter, VectorCounterCalculator<T>> implements CounterSpecificTrait {}

