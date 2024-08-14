package com.cachakka.streaming.metrics.core;


import com.cachakka.streaming.core.utils.Utils;
import org.joda.time.DateTime;

import java.util.Optional;

/**
 * Most common invokation context: just wraps the time-stampped data
 * @param <T>
 */
public class DefaultFlinkMetricsInvokationContext<T> implements FlinkMetricsInvokationContext<T, DefaultFlinkMetricsInvokationContext<T>> {

    private final T data;
    private final DateTime time;

    public DefaultFlinkMetricsInvokationContext(final T data){
        this(data, Utils.currentTime());
    }

    public DefaultFlinkMetricsInvokationContext(final T data, final DateTime time){
        this.data = data;
        this.time = time;
    }

    @Override
    public DateTime getInvokeTime() {
        return time;
    }

    @Override
    public Optional<T> getData() {
        return Optional.of(data);
    }

    @Override
    public DefaultFlinkMetricsInvokationContext<T> getReifiedSelf() {
        return this;
    }
}
