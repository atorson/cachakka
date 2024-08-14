package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.MeterSpecificTrait;
import org.apache.flink.metrics.Meter;

public final class VectorRateMeterCalculator<T> extends VectorFlinkMetricsCalculator<T,Void,Meter,VectorRateMeterCalculator<T>> implements MeterSpecificTrait {}
