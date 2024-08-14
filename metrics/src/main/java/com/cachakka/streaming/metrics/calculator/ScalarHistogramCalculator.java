package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.HistogramSpecificTrait;
import org.apache.flink.metrics.Histogram;

/**
 * Scala histogram calculator
 * @param <T>
 * @param <C>
 */
public class ScalarHistogramCalculator<T, C extends ScalarHistogramCalculator<T,C>> extends ScalarTimestampedFlinkMetricsCalculator<T, Histogram, C> implements HistogramSpecificTrait {}
