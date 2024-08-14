package com.cachakka.streaming.metrics.core;


import com.cachakka.streaming.core.utils.Utils;
import org.joda.time.DateTime;

import java.util.Optional;

/**
 * Interim context: passed to output metrics calculators before invoking the map/mapAsync/... transformation
 * Note: input data is leaking to the output metrics calculators - this is intentional
 * @param <IN> input type
 * @param <OUT> output type
 */
public class InterimFlinkMetricsInvokationContext<IN,OUT> implements FlinkMetricsInvokationContext<OUT, InterimFlinkMetricsInvokationContext<IN,OUT>> {

    private final IN data;
    private final DateTime time;

    public InterimFlinkMetricsInvokationContext(final IN data){
        this(data, Utils.currentTime());
    }

    public InterimFlinkMetricsInvokationContext(final IN data, final DateTime time){
        this.data = data;
        this.time = time;
    }

    @Override
    public DateTime getInvokeTime() {
        return time;
    }

    @Override
    public Optional<OUT> getData() {
        return Optional.empty();
    }

    public IN getInput(){
        return data;
    }

    @Override
    public InterimFlinkMetricsInvokationContext<IN, OUT> getReifiedSelf() {
        return this;
    }
}
