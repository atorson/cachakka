package com.cachakka.streaming.metrics.core;


import com.cachakka.streaming.core.api.StreamingFlowDataTypeAware;
import com.cachakka.streaming.metrics.calculator.AbstractFlinkMetricsCalculator;
import io.vavr.API;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Metric;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.vavr.API.$;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;

/**
 * Common interface for classes which encapsulate business logic related to a common group of streaming metrics
 * Supplier interface provides a definition of all the metrics (indexed by name-keys)
 * Consumer interface allows the plug the calculator into a streaming pipeline
 * @param <T> streaming data type (each event of this type will flow through & be consumed by the calculator)
 */
public interface FlinkMetricsCalculator<T> extends StreamingFlowDataTypeAware<T>, Consumer<FlinkMetricsInvokationContext<T,?>>, Supplier<Map<List<String>, Metric>>, Serializable {

    /**
     * Bootstrapping method
     * Can be used for smart/configuration-based filtering or dynamic vector metrics
     * Example: calculators can cache the RuntimeContext in their internal state and use it to dynamically register new meters
     * @param ctx Flink pipeline context
     */
    void bootstrap(@Nonnull RuntimeContext ctx);

    /**
     * Extracts a current snapshot of all metric values (used for persistence)
     * @return
     */
    default Map<List<String>, List<Long>> getMetricValuesSnapshot(){
        return Collections.emptyMap();
    }

    /**
     * Recovers the prior history of a meter on initialization (when Meter is re-created)
     * @param scope meter scope
     * @return list of history values (likely, just one value representing a convolution of priors)
     */
    default List<Long> recoverMeterHistory(List<String> scope){return Collections.emptyList();}


    /**
     * Helper method to make calculator aware of the streaming data type if they are not
     * @param typeInfo clazz
     * @return type-aware wrapper
     */
    default FlinkMetricsCalculator<T> with(@Nonnull final TypeInformation<T> typeInfo){
        class WrapperMetricsCalculator<T> extends AbstractFlinkMetricsCalculator<T, WrapperMetricsCalculator<T>> {

            private final FlinkMetricsCalculator<T> delegate;

            private WrapperMetricsCalculator(final TypeInformation<T> typeInformation,
                                             final FlinkMetricsCalculator<T> delegate){
                super();
                withTypeInfo(typeInformation);
                this.delegate = delegate;
            }

            @Override
            public void bootstrap(@Nonnull RuntimeContext ctx) {
                delegate.bootstrap(ctx);
            }

            @Override
            public void accept(FlinkMetricsInvokationContext<T,?> ctx) {
                delegate.accept(ctx);
            }

            @Override
            public Map<List<String>, Metric> get() {
                return delegate.get();
            }

            @Override
            public Map<List<String>, List<Long>> getMetricValuesSnapshot() {
                return delegate.getMetricValuesSnapshot();
            }

            @Override
            public List<Long> recoverMeterHistory(List<String> scope) {
                return delegate.recoverMeterHistory(scope);
            }
        }
        if (getStreamingDataType() != null) return this;
        return  Match(this).of(
                API.<AbstractFlinkMetricsCalculator<T, ? extends AbstractFlinkMetricsCalculator<T, ?>>, FlinkMetricsCalculator<T>>Case($(instanceOf(AbstractFlinkMetricsCalculator.class)), c -> c.withTypeInfo(typeInfo)),
                API.<FlinkMetricsCalculator<T>, FlinkMetricsCalculator<T>>Case($(), c -> new WrapperMetricsCalculator<>(typeInfo, this))
        );

    }
}
