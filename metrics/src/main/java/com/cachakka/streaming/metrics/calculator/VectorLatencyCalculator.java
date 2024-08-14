package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.core.utils.Utils;
import com.cachakka.streaming.metrics.core.FlinkMetricsInvokationContext;
import com.cachakka.streaming.metrics.core.HistogramSpecificTrait;
import com.cachakka.streaming.metrics.core.InterimFlinkMetricsInvokationContext;
import io.vavr.API;
import io.vavr.Function2;
import org.apache.flink.metrics.Histogram;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.Predicates.instanceOf;
import static java.util.function.Function.identity;


/**
 * Generalization of the simple scalar latency calculator
 * Computes latencies by keys (extracts keys from combination of input and output data)
 * @param <IN> input data type
 * @param <OUT> output data type
 */
public final class VectorLatencyCalculator<IN,OUT> extends VectorFlinkMetricsCalculator<OUT, InterimFlinkMetricsInvokationContext<IN,OUT>, Histogram, VectorLatencyCalculator<IN,OUT>> implements HistogramSpecificTrait {

    /**
     * Constructor
     * @param keyExtractor function to extract key from input and output data tuple
     */
    public VectorLatencyCalculator(@Nonnull final Function2<IN,OUT,List<String>> keyExtractor) {
       super();
       this.withEvaluator((ctx, st) -> {
            final Map<List<String>, Long> result = new HashMap<>();
            // set state on interim ticks
            API.<FlinkMetricsInvokationContext<OUT,?>>Match(ctx).<InterimFlinkMetricsInvokationContext<IN,OUT>>option(
                    Case($(instanceOf(InterimFlinkMetricsInvokationContext.class)), identity())
            ).toJavaOptional().ifPresent(st::set);
            // update result on non-interim ticks
            ctx.getData().flatMap((out) -> Optional.ofNullable(st.get()).map((s) ->
                    Collections.unmodifiableMap(Stream.of(
                            new AbstractMap.SimpleEntry<>(keyExtractor.apply(s.getInput(), out),
                                    Utils.currentTime().getMillis() - s.getInvokeTime().getMillis()))
                            .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())))))
                    .ifPresent((m) -> {
                        // setting the state to null on non-interim ticks: makes this calculator work correctly on input/sink/source calculator scenarios
                        st.set(null);
                        result.putAll(m);
                    });
            return result;
        });
    }
}
