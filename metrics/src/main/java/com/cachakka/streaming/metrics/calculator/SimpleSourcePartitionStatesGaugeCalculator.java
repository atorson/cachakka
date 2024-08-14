package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import io.vavr.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Simple single gauge reflecting a state of the given source partition
 * @param <T> streaming data type
 * @param <S> partition state data type
 */
public final class SimpleSourcePartitionStatesGaugeCalculator<T, S extends Serializable> extends SourcePartitionStatesGaugeCalculator<T,S,S,SimpleSourcePartitionStatesGaugeCalculator<T,S>> {

    public SimpleSourcePartitionStatesGaugeCalculator(){
        super();
    }

    @Override
    protected Map<List<String>,Supplier<S>> convertPartitionStates(List<PartitionedSourceFunction.SourcePartitionState<S>> states){
        return states.stream().map(s -> new Tuple2<>(s.getPartitionKey(), s)).collect(Collectors.toMap(t -> t._1, t -> t._2));
    }


}
