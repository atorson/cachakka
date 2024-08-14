package com.cachakka.streaming.metrics.calculator;


import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import io.vavr.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Composite multiple-gauge reflecting a state of the given source partition
 * @param <T> streaming data type
 */
public class CompositeSourcePartitionStatesGaugeCalculator<T> extends SourcePartitionStatesGaugeCalculator<T,Map<String,Supplier<? extends Serializable>>,Serializable,CompositeSourcePartitionStatesGaugeCalculator<T>> {

    public CompositeSourcePartitionStatesGaugeCalculator(){
        super();
    }

    @Override
    protected Map<List<String>,Supplier<Serializable>> convertPartitionStates(List<PartitionedSourceFunction.SourcePartitionState<Map<String,Supplier<? extends Serializable>>>> states){
        return states.stream().flatMap(s -> s.get().entrySet().stream()
                .map(e -> new Tuple2<List<String>,Supplier<Serializable>>(io.vavr.collection.Array.ofAll(s.getPartitionKey()).append(e.getKey()).toJavaList(),
                        e.getValue()::get))).collect(Collectors.toMap(t -> t._1, t -> t._2));
    }


}

