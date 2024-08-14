package com.cachakka.streaming.metrics.provider;


import com.cachakka.streaming.core.api.StreamingFunctionDataTypesAware;
import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import com.cachakka.streaming.metrics.calculator.AbstractFlinkMetricsCalculator;
import com.cachakka.streaming.metrics.core.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Metric;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provider of the 2-arg metered Flink function wrappers
 * @param <IN> input data type
 * @param <OUT> output data type
 * @param <OPER> type of 1-arg Flink function operator
 * @param <C> concrete sub-class of the provider (needed for strongly-typed builder implementation)
 */
abstract class MeteredFlinkOperator2Provider<IN,OUT,OPER extends Function, C extends MeteredFlinkOperator2Provider<IN, OUT, OPER,C>> extends MeteredFlinkOperatorProvider<OPER> implements StreamingFunctionDataTypesAware<IN, OUT> {

    protected TypeInformation<IN> capturedInputDataType;
    protected TypeInformation<OUT> capturedOutputDataType;
    protected FlinkReflectionUtils.ReflectionTypeExtractor<IN> inputTypeExtractor;
    protected FlinkReflectionUtils.ReflectionTypeExtractor<OUT> outputTypeExtractor;


    public MeteredFlinkOperator2Provider(){
        inputTypeExtractor = refinedExtractor(0);
        outputTypeExtractor = refinedExtractor(1);
    }

    private <T> FlinkReflectionUtils.ReflectionTypeExtractor<T> refinedExtractor(int position){
        final FlinkReflectionUtils.ReflectionTypeExtractor<T> blank = new FlinkReflectionUtils.ReflectionTypeExtractor<>();
        return blank.extractTypeInformation(this, MeteredFlinkOperator2Provider.class,position)
                .map((t) -> new FlinkReflectionUtils.ReflectionTypeExtractor<>(t)).getOrElse(blank);
    }


    /**
     * Fluent builder (with inferred type classes)
     * @param inputMetricsCalculator  metric calculator operating on input stream
     * @param outputMetricsCalculator metric calculator operating on output stream
     * @param delegateFunction        underlying Flink 'map' function to delegate to
     * @param userMetricGroupScope    user-scope for metric group (to add to the Flink-defined system-scope)
     */
    public C build(@Nonnull final OPER delegateFunction,
                   @Nonnull final Optional<FlinkMetricsCalculator<IN>> inputMetricsCalculator,
                   @Nonnull final Optional<FlinkMetricsCalculator<OUT>> outputMetricsCalculator,
                   @Nullable String... userMetricGroupScope) {
        return build(this.inputTypeExtractor, this.outputTypeExtractor, delegateFunction, inputMetricsCalculator, outputMetricsCalculator, userMetricGroupScope);
    }

    public C withUnionPersistentStateRedistribution(){
        this.useEvenSplitStateRedistribution = false;
        return (C) this;
    }

    // needed for fluent builder with inferred data type
    protected abstract Tuple2<TypeInformation<IN>,TypeInformation<OUT>> inferStreamingDataTypes();


    /**
     * Fluent builder (with captured type classes)
     * @param inputTypeInfo streaming input data type class
     * @param outputTypeInfo streaming output data type class
     * @param delegateFunction        underlying Flink 'map' function to delegate to
     * @param inputMetricsCalculator  metric calculator operating on input stream
     * @param outputMetricsCalculator metric calculator operating on output stream
     * @param userMetricGroupScope    user-scope for metric group (to add to the Flink-defined system-scope)
     */
    public C build(@Nonnull final TypeInformation<IN> inputTypeInfo,
                 @Nonnull final TypeInformation<OUT> outputTypeInfo,
                 @Nonnull final OPER delegateFunction,
                 @Nonnull final Optional<FlinkMetricsCalculator<IN>> inputMetricsCalculator,
                 @Nonnull final Optional<FlinkMetricsCalculator<OUT>> outputMetricsCalculator,
                 @Nonnull String... userMetricGroupScope){
        return build(new FlinkReflectionUtils.ReflectionTypeExtractor<>(inputTypeInfo), new FlinkReflectionUtils.ReflectionTypeExtractor<>(outputTypeInfo),
                delegateFunction, inputMetricsCalculator, outputMetricsCalculator, userMetricGroupScope);
    }

    private C build(@Nonnull final FlinkReflectionUtils.ReflectionTypeExtractor<IN> inputTypeExtractor,
                   @Nonnull final FlinkReflectionUtils.ReflectionTypeExtractor<OUT> outputTypeExtractor,
                   @Nonnull final OPER delegateFunction,
                   @Nonnull final Optional<FlinkMetricsCalculator<IN>> inputMetricsCalculator,
                   @Nonnull final Optional<FlinkMetricsCalculator<OUT>> outputMetricsCalculator,
                   @Nonnull String... userMetricGroupScope){

        this.inputTypeExtractor = inputTypeExtractor;
        this.outputTypeExtractor = outputTypeExtractor;
        this.innerFunction = delegateFunction;
        setScope(userMetricGroupScope);
        Tuple2<TypeInformation<IN>,TypeInformation<OUT>> typeInfos = inferStreamingDataTypes();
        capturedInputDataType = typeInfos.f0;
        capturedOutputDataType = typeInfos.f1;
        this.metricsCalculators = capturedInputDataType.equals(capturedOutputDataType) && !inputMetricsCalculator.isPresent() && outputMetricsCalculator.isPresent() ?
                FlinkMetricCalculatorsRegistry.newInstance().withCalculator(new StubMetricsCalculator(capturedInputDataType)).withCalculatorOption(outputMetricsCalculator.map(calc -> calc.with(capturedOutputDataType)))
                : FlinkMetricCalculatorsRegistry.newInstance().withCalculatorOption(inputMetricsCalculator.map(calc -> calc.with(capturedInputDataType))).withCalculatorOption(outputMetricsCalculator.map(calc -> calc.with(capturedOutputDataType)));

        return (C) this;
    }


    class StubMetricsCalculator<T> extends AbstractFlinkMetricsCalculator<T, StubMetricsCalculator<T>> {

        private StubMetricsCalculator(@Nonnull TypeInformation<T> typeInformation){
            super();
            withTypeInfo(typeInformation);
        }

        @Override
        public void accept(FlinkMetricsInvokationContext<T,?> ctx) {}

        @Override
        public Map<List<String>, Metric> get() {
            return Collections.emptyMap();
        }
    }

    @Override
    public TypeInformation<IN> getInputDataType() {
        return capturedInputDataType;
    }

    @Override
    public TypeInformation<OUT> getOutputDataType() {
        return capturedOutputDataType;
    }

    protected Optional<FlinkMetricsCalculator<IN>> getInputCalculator(){
        return metricsCalculators.retrieve(getInputDataType()).flatMap(calculators -> calculators.getOptional(0));
    }

    protected Optional<FlinkMetricsCalculator<OUT>> getOutputCalculator(){
        return metricsCalculators.retrieve(getOutputDataType()).flatMap(calculators -> calculators.size() > 1? calculators.getOptional(1): calculators.getOptional(0));
    }

    protected FlinkMetricsInvokationContext<IN,DefaultFlinkMetricsInvokationContext<IN>> wrapInputValue(IN data){
        return new DefaultFlinkMetricsInvokationContext<>(data);
    }

    protected FlinkMetricsInvokationContext<OUT,DefaultFlinkMetricsInvokationContext<OUT>> wrapOutputValue(OUT data){
        return new DefaultFlinkMetricsInvokationContext<>(data);
    }

    protected FlinkMetricsInvokationContext<OUT,InterimFlinkMetricsInvokationContext<IN,OUT>> wrapInterimValue(IN data){
        return new InterimFlinkMetricsInvokationContext<>(data);
    }

    protected abstract class MeteredFlinkOperator2Adapter extends MeteredFlinkOperatorAdapter implements MeteredFlinkFunction<OUT>, StreamingFunctionDataTypesAware<IN, OUT> {
        @Override
        public TypeInformation<IN> getInputDataType() {
            return capturedInputDataType;
        }

        @Override
        public TypeInformation<OUT> getOutputDataType() {
            return capturedOutputDataType;
        }

        @Override
        public FlinkMetricCalculatorsRegistry get() {
            return metricsCalculators;
        }
    }
}
