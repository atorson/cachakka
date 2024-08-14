package com.cachakka.streaming.metrics.provider;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Provider of the metered 'map' operators
 * @param <IN>
 * @param <OUT>
 */
public class MeteredFlinkMapProvider<IN,OUT> extends MeteredFlinkOperator2Provider<IN, OUT, MapFunction<IN,OUT>,MeteredFlinkMapProvider<IN,OUT>> {

    protected static final long serialVersionUID = 0;

    @Override
    protected Tuple2<TypeInformation<IN>,TypeInformation<OUT>> inferStreamingDataTypes() {
        return Tuple2.of(inputTypeExtractor.extractTypeInformation(innerFunction, MapFunction.class,0).get(),
                outputTypeExtractor.extractTypeInformation(innerFunction, MapFunction.class,1).get());
    }

    @Override
    public MapFunction<IN,OUT> get() {
        class MeteredFlinkMap extends MeteredFlinkOperator2Adapter implements MapFunction<IN,OUT>{

            @Override
            public OUT map(IN value) throws Exception {
                getInputCalculator().ifPresent(calc -> calc.accept(wrapInputValue(value)));
                getOutputCalculator().ifPresent(calc -> calc.accept(wrapInterimValue(value)));
                OUT result = innerFunction.map(value);
                getOutputCalculator().ifPresent(calc -> calc.accept(wrapOutputValue(result)));
                return result;
            }
        }
        return new MeteredFlinkMap();
    }
}
