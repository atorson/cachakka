package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import com.cachakka.streaming.metrics.core.ResettingCounter;
import com.cachakka.streaming.metrics.core.ResettingCounterSpecificTrait;
import io.vavr.Function1;
import io.vavr.Tuple2;
import org.apache.flink.api.common.functions.RuntimeContext;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class DistinctResettingVectorCounterCalculator<T,K> extends VectorFlinkMetricsCalculator<T,Map<List<String>, Tuple2<Long,Map<K, AtomicInteger>>>,ResettingCounter,DistinctResettingVectorCounterCalculator<T,K>> implements ResettingCounterSpecificTrait {

    @Override
    protected Map<List<String>, Tuple2<Long,Map<K, AtomicInteger>>> recoverState(RuntimeContext ctx) {
        Map<List<String>, Tuple2<Long,Map<K, AtomicInteger>>> superState = super.recoverState(ctx);
        return superState != null? superState: new ConcurrentHashMap<>();
    }


    public DistinctResettingVectorCounterCalculator(@Nonnull Function1<FlinkMetricsInvokationContext<T, ?>, Tuple2<K, List<String>>>  keyExtractor,
                                                    @Nonnull Function1<Integer, Long> evaluator){
        super();
        this.withEvaluator((x,s) -> {
            final Map<List<String>, Long> result = new HashMap<>();
            Tuple2<K, List<String>> key = keyExtractor.apply(x);
            List<String> groupKey = key != null? key._2: null;
            K itemKey = key != null? key._1: null;
            if (groupKey != null && itemKey != null && !groupKey.isEmpty()){
                 ResettingCounter meter = getMeters().get(groupKey);
                 Tuple2<Long, Map<K, AtomicInteger>> stateRecord = s.get().get(groupKey);
                 if (stateRecord == null){
                     // new state record
                     stateRecord = new Tuple2<>(null, new ConcurrentHashMap<>());
                 }
                 if (meter != null && stateRecord._1 == null){
                    // complete state record after meter has been created previously
                    stateRecord = new Tuple2<>(meter.getNextScheduledResetTime(), stateRecord._2);
                 }
                 Long time1 = stateRecord._1;
                 Long time2 = meter != null? meter.getNextScheduledResetTime(): null;
                 if (time1 != null && time2 != null && !time2.equals(time1)){
                      // flush state
                     stateRecord = new Tuple2<>(time2, new ConcurrentHashMap<>());
                 }
                 // update state map
                 s.get().put(groupKey, stateRecord);
                 AtomicInteger counter = stateRecord._2.computeIfAbsent(itemKey, (k) -> new AtomicInteger(0));
                 result.put(groupKey, evaluator.apply(counter.incrementAndGet()));
            }
            return result;
        });
    }
}