package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.MetricSpecificTrait;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Metric;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Parent of the strongly-typed, stateful metric calculators hierarchy
 * @param <T> streaming data type
 * @param <S> internal state data type
 * @param <M> type of metric
 * @param <C> concrete calculator sub-type
 */
public abstract class StatefulFlinkMetricsCalculator<T,S,M extends Metric, C extends StatefulFlinkMetricsCalculator<T,S,M,C>> extends AbstractFlinkMetricsCalculator<T,C> implements MetricSpecificTrait<M> {

    protected transient AtomicReference<S> state;

    protected StatefulFlinkMetricsCalculator(){
        super();
        this.state = new AtomicReference<>();
    }

    //ToDo: implement using Flink Metrics persistence query or throw FlinkBroadcast checkpoint
    /**
     * Recovers the last state value on bootstrap (when Flink job starts)
     * @param ctx runtime context
     * @return state value
     */
    protected S recoverState(RuntimeContext ctx){return null;}

    @Override
    public void bootstrap(@Nonnull RuntimeContext ctx) {
        synchronized (this) {
            this.state = new AtomicReference<>(recoverState(ctx));
            super.bootstrap(ctx);
         }
    }
}
