package com.cachakka.streaming.metrics.provider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Provider of the metered 'sink' operators
 * @param <IN>
 */
public class MeteredFlinkSinkProvider<IN> extends MeteredFlinkOperator1Provider<IN,SinkFunction<IN>, MeteredFlinkSinkProvider<IN>> {


    protected static final long serialVersionUID = 0;

    @Override
    protected TypeInformation<IN> inferStreamingDataType() {
        return extractor.extractTypeInformation(innerFunction, SinkFunction.class,0).get();
    }

    @Override
    public SinkFunction<IN> get() {
        class MeteredFlinkSink extends MeteredFlinkOperator1Adapter implements SinkFunction<IN> {

            @Override
            public void invoke(IN value, Context context) throws Exception {
                // there is really just one calculator in the set
                innerFunction.invoke(value, context);
                metricsCalculators.retrieve(getStreamingDataType()).ifPresent(calculators -> calculators.forEach(calc -> calc.accept(wrapValue(value))));
            }
        }
        return new MeteredFlinkSink();
    }
}
