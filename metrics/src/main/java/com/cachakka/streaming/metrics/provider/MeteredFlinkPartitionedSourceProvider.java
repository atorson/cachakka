package com.cachakka.streaming.metrics.provider;

import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import com.cachakka.streaming.metrics.calculator.SourcePartitionStatesGaugeCalculator;
import io.vavr.Function0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static java.util.function.Function.identity;


public class MeteredFlinkPartitionedSourceProvider<OUT,S> extends MeteredFlinkSourceProvider<OUT, PartitionedSourceFunction<OUT,S>, MeteredFlinkPartitionedSourceProvider<OUT,S>> {

    protected final Function0<SourcePartitionStatesGaugeCalculator<OUT,S,?,?>> partitionStateGaugeCalculatorProvider;

    public MeteredFlinkPartitionedSourceProvider(Function0<SourcePartitionStatesGaugeCalculator<OUT,S,?,?>> partitionStateGaugeCalculatorProvider){
        this.partitionStateGaugeCalculatorProvider = partitionStateGaugeCalculatorProvider;
    }

    protected class MeteredFlinkPartitionedSource extends MeteredFlinkSource implements PartitionedSourceFunction<OUT,S>{

        protected MeteredFlinkPartitionedSource(){
            super();
            metricsCalculators.withCalculator(partitionStateGaugeCalculatorProvider.get().withTypeInfo(capturedStreamingDataType));
        }

        protected Function0<Consumer<List<PartitionedSourceFunction.SourcePartitionState<S>>>> callback;

        @Override
        public void onPartitionAssignment(Function0<Consumer<List<SourcePartitionState<S>>>> callback) {
            this.callback = callback;
        }

        private Optional<SourcePartitionStatesGaugeCalculator<OUT,S,?,?>> getPartitionStatesGaugeCalculator(){
            return metricsCalculators.retrieve(capturedStreamingDataType).flatMap(e -> e.getOptional(e.size()-1)).flatMap(e ->
                    Match(e).<SourcePartitionStatesGaugeCalculator<OUT,S,?,?>>option(
                    Case($(instanceOf(SourcePartitionStatesGaugeCalculator.class)), identity())
            ).toJavaOptional());
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            final Function0<Consumer<List<PartitionedSourceFunction.SourcePartitionState<S>>>> externalCallback = callback;
            innerFunction.onPartitionAssignment(() -> (partitions) -> {
                if (externalCallback != null) {
                    externalCallback.apply().accept(partitions);
                }
                getPartitionStatesGaugeCalculator().ifPresent(c -> c.withPartitionStates(partitions));
            });
        }


        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            innerFunction.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            super.snapshotState(context);
            innerFunction.snapshotState(context);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
              super.initializeState(context);
              innerFunction.initializeState(context);
        }
    }

    @Override
    public PartitionedSourceFunction<OUT,S> get() {
        return new MeteredFlinkPartitionedSource();
    }
}
