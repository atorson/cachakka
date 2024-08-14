package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.MeterSpecificTrait;
import org.apache.flink.metrics.Meter;


/**
 * Scalar calculator that measures event/sec traffic rate, averaged over 1/5/15 min time window
 * @param <T>
 */
public final class SimpleRateMeterCalculator<T> extends ScalarFlinkMetricsCalculator<T,Void, Meter,SimpleRateMeterCalculator<T>> implements MeterSpecificTrait {}
