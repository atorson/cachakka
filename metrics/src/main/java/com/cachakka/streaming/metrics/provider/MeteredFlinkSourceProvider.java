package com.cachakka.streaming.metrics.provider;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.function.Consumer;

/**
 * Provider of the metered 'source' operators
 * @param <OUT>
 */
public abstract class MeteredFlinkSourceProvider<OUT, OPER extends SourceFunction<OUT>, C extends MeteredFlinkSourceProvider<OUT,OPER,C>> extends MeteredFlinkOperator1Provider<OUT, OPER, C> {

    protected static final long serialVersionUID = 0;

    @Override
    protected TypeInformation<OUT> inferStreamingDataType() {
        return extractor.extractTypeInformation(innerFunction, SourceFunction.class,0).get();
    }

    // attach a consumer aspect/callback to the context to pass to the inner function
    protected class CallbackDecoratedSourceContext<OUT> implements SourceFunction.SourceContext<OUT> {

        final Consumer<OUT> consumer;
        final SourceFunction.SourceContext<OUT> delegate;

        private CallbackDecoratedSourceContext(final Consumer<OUT> consumer, final SourceFunction.SourceContext<OUT> ctx){
            this.consumer = consumer;
            this.delegate = ctx;
        }

        @Override
        public void collect(OUT element) {
            delegate.collect(element);
            consumer.accept(element);
        }

        @Override
        @PublicEvolving
        public void collectWithTimestamp(OUT element, long timestamp) {
            delegate.collectWithTimestamp(element, timestamp);
            consumer.accept(element);
        }

        @Override
        @PublicEvolving
        public void emitWatermark(Watermark mark) {
            delegate.emitWatermark(mark);
        }

        @Override
        @PublicEvolving
        public void markAsTemporarilyIdle() {
            delegate.markAsTemporarilyIdle();
        }

        @Override
        public Object getCheckpointLock() {
            return delegate.getCheckpointLock();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    protected abstract class MeteredFlinkSource extends MeteredFlinkOperator1Adapter implements SourceFunction<OUT> {

        @Override
        public void run(SourceContext<OUT> ctx) throws Exception{
            innerFunction.run(new CallbackDecoratedSourceContext<>((item) -> metricsCalculators.retrieve(getStreamingDataType()).ifPresent(calculators -> calculators.forEach(calc -> calc.accept(wrapValue(item)))), ctx));
        }

        @Override
        public void cancel() {
            innerFunction.cancel();
        }
    }

}