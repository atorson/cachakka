package com.cachakka.streaming.core.utils;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;

public class AggregateIntoArrayListWindowFunction<IN> implements AggregateFunction<IN, ArrayList<IN>, ArrayList<IN>> {

    @Override
    public ArrayList<IN> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public ArrayList<IN> add(IN value, ArrayList<IN> accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public ArrayList<IN> getResult(ArrayList<IN> accumulator) {
        return accumulator;
    }

    @Override
    public ArrayList<IN> merge(ArrayList<IN> a, ArrayList<IN> b) {
        a.addAll(b);
        return a;
    }

}
