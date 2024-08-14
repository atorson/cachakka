package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import io.vavr.Function2;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Calculator that measures inter-arrival time distribution of the streaming flow traffic scale
 * @param <T>
 */
public final class SimpleTrafficHistogramCalculator<T> extends ScalarHistogramCalculator<T, SimpleTrafficHistogramCalculator<T>> {

    public SimpleTrafficHistogramCalculator(){
        super();
        final Function2<FlinkMetricsInvokationContext<T,?>, AtomicReference<DateTime>, Long> parent = evaluator;
        evaluator = (x,s) -> {
            DateTime curr = s.get();
            Long value = parent.apply(x,s);
            if (value == null){
                // only happens in output calculators in the iterim context - just ignore it and don't let it tick the timer
                s.set(curr);
                return null;
            } else {
                // return inter-arrival time in millis
               DateTime newVal = s.get();
               return newVal != null && curr != null? newVal.getMillis()-curr.getMillis(): null;
            }
        };
    }
}
