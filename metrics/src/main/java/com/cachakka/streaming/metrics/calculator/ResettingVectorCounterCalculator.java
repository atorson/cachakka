package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.metrics.core.ResettingCounter;
import com.cachakka.streaming.metrics.core.ResettingCounterSpecificTrait;

/**
 * Vector counter calculator that resets itself after a specified time period
 * @param <T> data type
 */
public class ResettingVectorCounterCalculator<T> extends VectorFlinkMetricsCalculator<T, Void, ResettingCounter, ResettingVectorCounterCalculator<T>> implements ResettingCounterSpecificTrait {}


