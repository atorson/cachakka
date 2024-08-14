package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.FlinkMetricCalculatorsRegistry;
import com.cachakka.streaming.metrics.core.FlinkMetricsCalculator;
import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Metric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Composite metric calculator implementation that bundles a collection of atomic calculator together
 * @param <T> streaming data type
 */
public class CompositeFlinkMetricsCalculator<T> extends AbstractFlinkMetricsCalculator<T, CompositeFlinkMetricsCalculator<T>> implements FlinkMetricCalculatorsRegistry.Entry<T> {

    protected static final long serialVersionUID = 0;
    protected final CopyOnWriteArrayList<FlinkMetricsCalculator<T>> innerList;

    public CompositeFlinkMetricsCalculator(){
        super();
        innerList = new CopyOnWriteArrayList<>();
    }

    @Override
    public synchronized CompositeFlinkMetricsCalculator<T> withTypeInfo(@Nonnull TypeInformation<T> typeInfo) {
        innerList.replaceAll(c -> c.with(typeInfo));
        return super.withTypeInfo(typeInfo);
    }

    /**
     * Fluent builder API
     * @param calc metrics calculator
     * @return self
     */
    public CompositeFlinkMetricsCalculator<T> withCalculator(@Nonnull FlinkMetricsCalculator<T> calc) {
        add(calc);
        return this;
    }

    @Override
    public void bootstrap(@Nonnull RuntimeContext ctx) {
        forEach(c -> c.bootstrap(ctx));
    }

    @Override
    public void add(FlinkMetricsCalculator<T> calculator) {
        innerList.add(calculator);
    }

    @Nullable
    @Override
    public FlinkMetricsCalculator<T> get(int index) throws IndexOutOfBoundsException {
        return innerList.get(index);
    }

    @Override
    public void forEach(Consumer<FlinkMetricsCalculator<T>> consumer) {
        innerList.forEach(consumer);
    }

    @Override
    public int size() {
        return innerList.size();
    }

    @Override
    public void accept(FlinkMetricsInvokationContext<T,?> ctx) {
        innerList.forEach((calculator) -> calculator.accept(ctx));
    }

    @Override
    public Map<List<String>, Metric> get() {
        return innerList.stream().flatMap((calculator) -> calculator.get().entrySet().stream()).collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue()));
    }

    @Override
    public Map<List<String>, List<Long>> getMetricValuesSnapshot() {
        return getCombinedMetricValuesSnapshot();
    }

    @Override
    public List<Long> recoverMeterHistory(List<String> scope) {
        return recoverCombinedMeterHistory(scope);
    }
}
