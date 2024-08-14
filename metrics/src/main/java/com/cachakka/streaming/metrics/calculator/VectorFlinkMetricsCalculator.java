package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import io.vavr.Function2;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Metric;

import javax.annotation.Nonnull;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Parent of the vectore/multi-dimensional metric calculators hierarchy
 * New dimensions/scope are resolved dynamically, from input data
 * @param <T> streaming data type
 * @param <S> internal state data type
 * @param <M> type of metric
 * @param <C> concrete calculator sub-type
 */
public abstract class VectorFlinkMetricsCalculator <T, S, M extends Metric, C extends VectorFlinkMetricsCalculator<T,S,M,C>> extends StatefulFlinkMetricsCalculator<T,S,M,C> {

    protected transient AtomicReference<RuntimeContext> ctx;
    protected Function2<FlinkMetricsInvokationContext<T,?>,AtomicReference<S>, Map<List<String>, Long>> evaluator;
    private transient ConcurrentHashMap<List<String>, M> meters;

    protected VectorFlinkMetricsCalculator(){
        super();
        this.ctx = new AtomicReference<>();
        this.meters = new ConcurrentHashMap<>();
        this.evaluator = (x,s) -> Collections.emptyMap();
    }

    protected synchronized M getMeter(final List<String> scope){
        if (scope.isEmpty()) throw new IllegalArgumentException("Empty metric scope");
        return getMeters().computeIfAbsent(scope, k -> {
            List<Long> history = recoverMeterHistory(k);
            final M meter = createMetric(scope);
            history.forEach(v -> actuateMetric(meter, v));
            return meter;
        });
    }

    protected synchronized ConcurrentHashMap<List<String>, M> getMeters(){
        if (meters == null){
            meters = new ConcurrentHashMap<>();
        }
        return meters;
    }

    public C withEvaluator(@Nonnull Function2<FlinkMetricsInvokationContext<T,?>, AtomicReference<S>, Map<List<String>, Long>> evaluator){
        synchronized (this) {
            this.evaluator = evaluator;
            return (C) this;
        }
    }

    /**
     * Simple evaluator builder: assumes that each input data piece maps only to one metric scope key
     * @param keyExtractor scope key extractor function (note the state param signature: it does not modify state)
     * @param evaluator scalar metric evaluator function
     * @param <E> any sub-type that holds the scope key (can hold extra data digest from FlinkMetricsInvokationContext)
     * @return
     */
    public <E extends Supplier<List<String>>> C withSimpleEvaluator(@Nonnull Function2<FlinkMetricsInvokationContext<T,?>,S, E> keyExtractor,
                                                                    @Nonnull Function2<E, AtomicReference<S>, Long> evaluator){
        return this.withEvaluator((x,s) -> {
              E interim = keyExtractor.apply(x, s.get());
              if (interim != null){
                 List<String> key = interim.get();
                 if (key != null && !key.isEmpty()){
                     Long value = evaluator.apply(interim, s);
                     if (value != null) {
                         return Collections.unmodifiableMap(Stream.of(
                                 new SimpleEntry<>(key, value))
                                 .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));
                     }
                 }
              }
              return (Collections.emptyMap());
        });
    }

    @Override
    public Map<List<String>, Metric> get() {
        return Collections.unmodifiableMap(getMeters());
    }

    @Override
    public void bootstrap(@Nonnull RuntimeContext ctx) {
        this.ctx = new AtomicReference<>(ctx);
        super.bootstrap(ctx);
    }

    @Override
    public void accept(FlinkMetricsInvokationContext<T,?> ctx) {
        synchronized (this) {
            Map<List<String>, Long> results = evaluator.apply(ctx, state);
            Set<List<String>> newKeys = new HashSet<>(com.google.common.collect.Sets.difference(results.keySet(), getMeters().keySet()));
            results.forEach((k, v) -> {
                if (!k.isEmpty() && v != null) {
                    actuateMetric(getMeter(k), v);
                }
            });
            registerMetrics(Collections.unmodifiableMap(newKeys.stream()
                    .map(k -> new SimpleEntry<>(k, getMeter(k)))
                    .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue()))), this.ctx.get());
        }
    }
}
