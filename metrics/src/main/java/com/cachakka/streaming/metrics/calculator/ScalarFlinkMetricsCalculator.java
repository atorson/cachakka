package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import io.vavr.Function2;
import org.apache.flink.metrics.Metric;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parent of scalar calculators hierarchy
 * Holds just one metric, with scope provided at construction time
 * @param <T> streaming data type
 * @param <S> internal state data type
 * @param <M> type of metric
 * @param <C> concrete calculator sub-type
 */
public abstract class ScalarFlinkMetricsCalculator<T, S , M extends Metric, C extends ScalarFlinkMetricsCalculator<T,S,M,C>> extends StatefulFlinkMetricsCalculator<T,S,M,C> {

    protected List<String> nameScope;
    protected Function2<FlinkMetricsInvokationContext<T,?>,AtomicReference<S>, Long> evaluator;
    private transient M meter;

    protected ScalarFlinkMetricsCalculator(){
        super();
        this.nameScope = Collections.emptyList();
        this.evaluator = (x,s) -> {
          if (x.getData().isPresent()) {
              return 1L;
          } else {
              return null;
          }
        };
    }

    public synchronized C withScope(@Nonnull String ... scope){
        List<String> scopeList = Preconditions.checkNotNull(scope, "Null Flink metrics calculator scope").length == 0? Arrays.asList(this.getClass().getCanonicalName() + "-" + UUID.randomUUID()): Arrays.asList(scope);
        scopeList.removeAll(Arrays.asList(null,""));
        this.nameScope = scopeList;
        return (C) this;
    }

    public synchronized C withEvaluator(@Nonnull Function2<FlinkMetricsInvokationContext<T,?>, AtomicReference<S>, Long> evaluator){
        this.evaluator = evaluator;
        return (C) this;
    }

    protected synchronized M getMeter(){
        if (meter == null){
            List<Long> history = recoverMeterHistory(nameScope);
            meter = createMetric(nameScope);
            history.forEach(v -> actuateMetric(meter, v));
        }
        return meter;
    }

    @Override
    public Map<List<String>, Metric> get() {
        return Collections.unmodifiableMap(Stream.of(
                new AbstractMap.SimpleEntry<>(nameScope, getMeter()))
                .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));
    }

    @Override
    public void accept(FlinkMetricsInvokationContext<T,?> ctx) {
       synchronized (this) {
           Long value = evaluator.apply(ctx, state);
           if (value != null) actuateMetric(getMeter(), value);
       }
    }
}
