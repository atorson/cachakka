package com.cachakka.streaming.metrics.provider;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Provider of the metered 'mapAsync' operators
 * @param <IN>
 * @param <OUT>
 */
public class MeteredFlinkAsyncProvider<IN,OUT> extends MeteredFlinkOperator2Provider<IN, OUT,AsyncFunction<IN, OUT>,MeteredFlinkAsyncProvider<IN,OUT>> {

    protected static final long serialVersionUID = 0;

    @Override
    protected Tuple2<TypeInformation<IN>,TypeInformation<OUT>> inferStreamingDataTypes() {
        return Tuple2.of(inputTypeExtractor.extractTypeInformation(innerFunction, AsyncFunction.class,0).get(),
                outputTypeExtractor.extractTypeInformation(innerFunction, AsyncFunction.class,1).get());
    }

    @Override
    public AsyncFunction<IN, OUT> get() {
        // Flink underneath implements ResultFuture as a richly decorated CompletableFuture. They could've exposed it - but they didn't
        class FutureDecoratedAsyncCollector<OUT> implements ResultFuture<OUT>{

            private final CompletableFuture<Collection<OUT>> innerFuture;

            private FutureDecoratedAsyncCollector(){
                innerFuture = new CompletableFuture<>();
            }

            @Override
            public void complete(Collection<OUT> result) {
               innerFuture.complete(result);
            }

            @Override
            public void completeExceptionally(Throwable error) {
               innerFuture.completeExceptionally(error);
            }
        }
        class MeteredFlinkAsync extends MeteredFlinkOperator2Adapter implements AsyncFunction<IN, OUT> {

            private Optional<RichAsyncFunction> getInnerRichAsyncFunctionOptional(){
               return getInnerRichFunctionOptionalTyped(RichAsyncFunction.class);
            }

            // need to override it because asycFunction has special/restricted context in Flink
            @Override
            public void setRuntimeContext(RuntimeContext ctx) {
                super.setRuntimeContext(ctx);
                getInnerRichAsyncFunctionOptional().ifPresent(func -> super.setRuntimeContext(func.getRuntimeContext()));
            }



            @Override
            public void asyncInvoke(IN input, ResultFuture<OUT> future) throws Exception {
                FutureDecoratedAsyncCollector<OUT> delegate = new FutureDecoratedAsyncCollector<>();
                delegate.innerFuture.whenComplete((results, exc) -> {
                    if (exc == null){
                        future.complete(results);
                        getOutputCalculator().ifPresent(calc -> results.forEach(r -> calc.accept(wrapOutputValue(r))));
                    } else {
                        future.completeExceptionally(exc);
                    }
                });
                getInputCalculator().ifPresent(calc -> calc.accept(wrapInputValue(input)));
                getOutputCalculator().ifPresent(calc -> calc.accept(wrapInterimValue(input)));
                innerFunction.asyncInvoke(input, delegate);
            }
        }
        return new MeteredFlinkAsync();
    }
}
