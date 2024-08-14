package com.cachakka.streaming.core.api;

import io.vavr.Function0;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;


public interface PartitionedSourceFunction<T, S> extends ParallelSourceFunction<T>, ResultTypeQueryable<T>, CheckpointListener, CheckpointedFunction {

    void onPartitionAssignment(Function0<Consumer<List<SourcePartitionState<S>>>> callback);

    interface SourcePartitionState<S> extends Supplier<S> {

        List<String> getPartitionKey();

    }
}
