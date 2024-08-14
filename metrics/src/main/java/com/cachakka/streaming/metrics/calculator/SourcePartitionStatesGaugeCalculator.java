package com.cachakka.streaming.metrics.calculator;

import com.google.common.base.Preconditions;
import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Gauges reflecting a state of the given source partition
 * @param <T> streaming data type
 * @param <S> partition state data type
 * @param <G> gauge data type
 * @param <C> concrete child type
 */
public abstract class SourcePartitionStatesGaugeCalculator<T,S,G extends Serializable,C> extends VectorFlinkMetricsCalculator<T,Map<List<String>,Supplier<G>>,Gauge<G>,SourcePartitionStatesGaugeCalculator<T,S,G,C>> {


    protected SourcePartitionStatesGaugeCalculator(){
        super();
        this.withEvaluator((x,s) -> Collections.emptyMap());
    }

    @Override
    protected Map<List<String>,Supplier<G>> recoverState(RuntimeContext ctx) {
        return new ConcurrentHashMap<>();
    }

    public void withPartitionStates(List<PartitionedSourceFunction.SourcePartitionState<S>> states){
        Map<List<String>,Supplier<G>> map = Preconditions.checkNotNull(state.get(), "Partition states map is null");
        //flush state: we have a new list of assigned partitions
        map.clear();
        map.putAll(convertPartitionStates(states));
        Set<List<String>> newKeys = new HashSet<>(com.google.common.collect.Sets.difference(map.keySet(), getMeters().keySet()));
        // make sure all gauges are created
        map.forEach((k,v) -> getMeter(k));
        // only register new keys, in case new partitions are added dynamically
        // the removed partitions will still have gauges registered - but state does not have suppliers for them anymore - so it is fine
        registerMetrics(get().entrySet().stream()
                .filter(e -> newKeys.contains(e.getKey())).collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue())), ctx.get());
    }

    protected abstract Map<List<String>,Supplier<G>> convertPartitionStates(List<PartitionedSourceFunction.SourcePartitionState<S>> states);


    @Override
    public void actuateMetric(Gauge<G> metric, Long result) {}

    // only holds a scope which never changes. As long as state is kept up to date - should work just fine
    class CalculatorStateBackedGaguge implements Gauge<G> {

        private final List<String> scope;

        CalculatorStateBackedGaguge(List<String> scope){
            this.scope = scope;
        }

        @Override
        public G getValue() {
            Supplier<G> supplier = state.get().get(scope);
            // if supplier is gone - return null (won't be reported by Reporter anymore
            return supplier != null? supplier.get(): null;
        }
    }

    @Override
    public Gauge<G> createMetric(List<String> scope) {

        return new CalculatorStateBackedGaguge(scope);
    }

}
