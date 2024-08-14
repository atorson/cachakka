package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import com.cachakka.streaming.metrics.core.ResettingCounter;
import com.cachakka.streaming.metrics.core.ResettingCounterSpecificTrait;
import io.vavr.Function1;
import io.vavr.Tuple2;
import org.apache.flink.api.common.functions.RuntimeContext;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Stateful counter that keeps track of distinct counts per extracted keys
 * State includes a set of visited keys: only new keys count
 * Counter resets to zero at a given interval in days; state is flushed away too
 * @param <T> streaming data type
 * @param <K> key data type
 */
public final class DistinctResettingEventCounterCalculator<T, K> extends ScalarFlinkMetricsCalculator<T,Tuple2<Long,Map<K, AtomicInteger>>,ResettingCounter,DistinctResettingEventCounterCalculator<T,K>> implements ResettingCounterSpecificTrait {

    @Override
    protected Tuple2<Long, Map<K, AtomicInteger>> recoverState(RuntimeContext ctx) {
        Tuple2<Long, Map<K, AtomicInteger>> superState = super.recoverState(ctx);
        return superState != null? superState: new Tuple2<>(getMeter().getNextScheduledResetTime(), new ConcurrentHashMap<>());
    }

    /**
     * Constructor
     * @param keyExtractor extracts distinct key
     * @param evaluator provides incremental evaluation, depending on the total count of occurences of the key
     */
    public DistinctResettingEventCounterCalculator(@Nonnull final Function1<FlinkMetricsInvokationContext<T, ?>,K> keyExtractor,
                                                   @Nonnull final Function1<Integer,Long> evaluator){
        super();
        this.evaluator = (x,s) -> {
            Long nextScheduledResetTime = getMeter().getNextScheduledResetTime();
            if (!nextScheduledResetTime.equals(s.get()._1)){
                //flush state
                s.set(new Tuple2<>(nextScheduledResetTime, new ConcurrentHashMap<>()));
            }
            K key = keyExtractor.apply(x);
            if (key != null) {
                AtomicInteger counter = s.get()._2.computeIfAbsent(key, (k) -> new AtomicInteger(0));
                return evaluator.apply(counter.incrementAndGet());
            }
            return null;
        };
    }

}
