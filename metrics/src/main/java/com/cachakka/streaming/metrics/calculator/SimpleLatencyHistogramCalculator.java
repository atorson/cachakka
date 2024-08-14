package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import io.vavr.Function2;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Calculator that captures histogram of latency of Flink transformation operators
 * @param <T>
 */
public final class SimpleLatencyHistogramCalculator<T> extends ScalarHistogramCalculator<T, SimpleLatencyHistogramCalculator<T>> {

    public SimpleLatencyHistogramCalculator() {
        super();
        final Function2<FlinkMetricsInvokationContext<T, ?>, AtomicReference<DateTime>, Long> parent = evaluator;
        evaluator = (x, s) -> {
            DateTime curr = s.get();
            Long value = parent.apply(x, s);
            if (value != null) {
                // ignore regular ticks and set the state to null on them
                // only interim tick matter
                DateTime newVal = s.get();
                s.set(null);
                // if curr is primed - return
                return newVal != null && curr != null? newVal.getMillis()-curr.getMillis(): null;
            }
            return null;
        };
    }
}
