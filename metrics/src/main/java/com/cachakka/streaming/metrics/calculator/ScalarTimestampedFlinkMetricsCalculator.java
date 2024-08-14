package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.core.utils.Utils;
import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import io.vavr.Function2;
import org.apache.flink.metrics.Metric;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Scalar metrics calculator which holds a single timestamp as its internal state
 * @param <T>
 * @param <M>
 * @param <C>
 */
public abstract class ScalarTimestampedFlinkMetricsCalculator<T,M extends Metric, C extends ScalarTimestampedFlinkMetricsCalculator<T,M,C>> extends ScalarFlinkMetricsCalculator<T,DateTime, M, C> {

    protected ScalarTimestampedFlinkMetricsCalculator(){
        super();
        final Function2<FlinkMetricsInvokationContext<T,?>, AtomicReference<DateTime>, Long> parent = evaluator;
        evaluator = (x,s) -> {
            Long value = parent.apply(x,s);
            s.set(Utils.currentTime());
            return value;
        };

    }

}
