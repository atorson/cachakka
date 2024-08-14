package com.cachakka.streaming.metrics.calculator;


import com.google.common.collect.ImmutableMap;
import com.google.gson.internal.Streams;
import com.cachakka.streaming.metrics.core.ResettingCounter;
import com.cachakka.streaming.metrics.core.ResettingCounterSpecificTrait;
import com.cachakka.streaming.metrics.utils.MetricUtils;
import io.vavr.control.Try;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Scalar counter calculator that resets itself after a fixed time period (1 day, by default)
 * @param <T> data type
 */
public class ResettingEventCounterCalculator<T> extends ScalarFlinkMetricsCalculator<T,Void, ResettingCounter,ResettingEventCounterCalculator<T>> implements ResettingCounterSpecificTrait{

    protected transient AtomicReference<RuntimeContext> ctx;

    private static final Logger logger = LoggerFactory.getLogger(ResettingEventCounterCalculator.class);

    public ResettingEventCounterCalculator(){
        super();
        ctx = new AtomicReference<>();
    }

    @Override
    public void bootstrap(@Nonnull RuntimeContext ctx) {
        this.ctx = new AtomicReference<>(ctx);
        super.bootstrap(ctx);
    }

    @Override
    public Map<List<String>, List<Long>> getMetricValuesSnapshot() {
        return ImmutableMap.of(nameScope, Collections.singletonList(getMeter().getCount()));
    }


    @Override
    public List<Long> recoverMeterHistory(List<String> scope) {
        List<Long> result = Try.of(() ->Preconditions.checkNotNull(
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(ctx.get().getListState(MetricUtils.METRIC_VALUES_STATE).get().iterator(), Spliterator.ORDERED),
                        false).flatMap(x -> Optional.ofNullable(x.get(scope)).orElse(Collections.emptyList()).stream()).collect(Collectors.toList())))
                .getOrElse(() -> super.recoverMeterHistory(scope));
        if (!result.isEmpty()){
            logger.info(String.format("Recovered meter %s values history %s", scope, result));
        }
        return result;
    }
}
