package com.cachakka.streaming.metrics.provider;


import com.cachakka.streaming.core.api.StreamingFlowDataTypeAware;
import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import com.cachakka.streaming.metrics.core.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Provider of 1-arg metered Flink function wrappers
 * @param <T> type of streaming data argument
 * @param <OPER> type of 1-arg Flink function operator
 * @param <C> concrete sub-class of the provider (needed for strongly-typed builder implementation)
 */
abstract class MeteredFlinkOperator1Provider<T, OPER extends Function, C extends MeteredFlinkOperator1Provider<T,OPER,C>> extends MeteredFlinkOperatorProvider<OPER> implements StreamingFlowDataTypeAware<T> {

    protected TypeInformation<T> capturedStreamingDataType;
    protected FlinkReflectionUtils.ReflectionTypeExtractor<T> extractor;

    public MeteredFlinkOperator1Provider(){
        final FlinkReflectionUtils.ReflectionTypeExtractor<T> blank = new FlinkReflectionUtils.ReflectionTypeExtractor<>();
        extractor = blank.extractTypeInformation(this, MeteredFlinkOperator1Provider.class,0)
                .map((t) -> new FlinkReflectionUtils.ReflectionTypeExtractor<>(t)).getOrElse(blank);
    }

    /**
     * Fluent builder (with inferred class)
     * @param metricsCalculator metric calculator operating on sink input stream
     * @param delegateFunction underlying Flink sink to delegate to
     * @param userMetricGroupScope user-scope for metric group (to add to the Flink-defined system-scope)
     */
    public C build(@Nonnull final OPER delegateFunction,
                   @Nonnull final Optional<FlinkMetricsCalculator<T>> metricsCalculator,
                   @Nonnull String ... userMetricGroupScope) {
       return build(this.extractor, delegateFunction, metricsCalculator, userMetricGroupScope);
    }

    // needed for fluent builder with inferred data type
    protected abstract TypeInformation<T> inferStreamingDataType();

    /**
     * Fluent builder (with captured class)
     * @param typeInfo streaming data type class
     * @param delegateFunction underlying Flink sink to delegate to
     * @param metricsCalculator metric calculator operating on sink input stream
     * @param userMetricGroupScope user-scope for metric group (to add to the Flink-defined system-scope)
     */
    public C build(@Nonnull final TypeInformation<T> typeInfo,
                   @Nonnull final OPER delegateFunction,
                   @Nonnull final Optional<FlinkMetricsCalculator<T>> metricsCalculator,
                   @Nonnull String ... userMetricGroupScope){
        return build(new FlinkReflectionUtils.ReflectionTypeExtractor<>(typeInfo), delegateFunction, metricsCalculator, userMetricGroupScope);
    }

    private C build(@Nonnull final FlinkReflectionUtils.ReflectionTypeExtractor<T> typeExtractor,
                    @Nonnull final OPER delegateFunction,
                    @Nonnull final Optional<FlinkMetricsCalculator<T>> metricsCalculator,
                    @Nonnull String ... userMetricGroupScope){
        this.innerFunction = delegateFunction;
        setScope(userMetricGroupScope);
        this.extractor = typeExtractor;
        this.capturedStreamingDataType = inferStreamingDataType();
        this.metricsCalculators = FlinkMetricCalculatorsRegistry.newInstance().withCalculatorOption(metricsCalculator.map(calc -> calc.with(capturedStreamingDataType)));
        return (C) this;
    }

    public C withUnionPersistentStateRedistribution(){
        this.useEvenSplitStateRedistribution = false;
        return (C) this;
    }

    @Override
    public TypeInformation<T> getStreamingDataType() {
        return capturedStreamingDataType;
    }

    protected abstract class MeteredFlinkOperator1Adapter extends MeteredFlinkOperatorAdapter implements MeteredFlinkFunction<T> {
        @Override
        public TypeInformation<T> getStreamingDataType() {
            return capturedStreamingDataType;
        }

        @Override
        public FlinkMetricCalculatorsRegistry get() {
            return metricsCalculators;
        }
    }

    protected FlinkMetricsInvokationContext<T,DefaultFlinkMetricsInvokationContext<T>> wrapValue(T data){
        return new DefaultFlinkMetricsInvokationContext<>(data);
    }
}
