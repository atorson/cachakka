package com.cachakka.streaming.metrics.provider;



import com.cachakka.streaming.metrics.core.FlinkMetricCalculatorsRegistry;
import com.cachakka.streaming.metrics.utils.MetricUtils;
import io.vavr.API;
import io.vavr.CheckedRunnable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.types.Value;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static io.vavr.Predicates.is;


/**
 * A wrapper provider/supplier class, encapsulating a Flink operator decorated with a metrics calculator attached to it
 * The provided class instance, in turn, implements the underlying Flink operator interface and can be used (as a replacement) anywhere the underlying operator is employed
 * @param <OPER> specific class of the underlying Flink operator implementation
 */
abstract class MeteredFlinkOperatorProvider<OPER extends Function> implements Supplier<OPER>, Serializable {

    protected OPER innerFunction;
    protected FlinkMetricCalculatorsRegistry metricsCalculators;
    protected List<String> userMetricGroupScope;
    protected boolean useEvenSplitStateRedistribution = true;



    private static final Logger logger = LoggerFactory.getLogger(MeteredFlinkOperatorProvider.class);

    /**
     * Smart setter
     * @param userMetricGroupScope user-scope for metric group (to add to the Flink-defined system-scope)
     */
    protected void setScope(@Nonnull String... userMetricGroupScope){
        List<String> scopeList = Preconditions.checkNotNull(userMetricGroupScope, "Null Flink operator metrics scope").length == 0? Arrays.asList("UserMetricsScope-" + UUID.randomUUID()): Arrays.asList(userMetricGroupScope);
        scopeList.removeAll(Arrays.asList(null,""));
        this.userMetricGroupScope = scopeList;
    }

    /**
     * This runner implements decoupled trigger then run operations
     * The use-case is to wrap throwing operations defined within monadic/pipelined functional blocks that do not tolerate exceptions
     * Then, run such operations (only if triggered!) outside of the monadic blocks where exceptions can be checked
     */
    protected class CheckedRunnableRunnerOnce implements Consumer<CheckedRunnable>, CheckedRunnable {

        private final AtomicBoolean shouldRun = new AtomicBoolean(false);
        private final AtomicReference<CheckedRunnable> innerRunnable = new AtomicReference<>();

        @Override
        public void accept(CheckedRunnable runnable) {
           synchronized (this) {
               if (runnable != null) {
                   innerRunnable.set(runnable);
                   shouldRun.set(true);
               }
           }
        }

        @Override
        public void run() throws Exception{
            synchronized (this) {
                if (shouldRun.get()) {
                    CheckedRunnable runnable = innerRunnable.get();
                    shouldRun.set(false);
                    innerRunnable.set(null);
                    try {
                        runnable.run();
                    } catch (Throwable throwable) {
                        throw new Exception(throwable);
                    }
                }
            }
        }
    }

    protected static class ScopedRestrictedRuntimeContext implements RuntimeContext{

        protected final RuntimeContext runtimeContext;
        protected final MetricGroup scopedGroup;
        protected final AtomicReference<ListState<Map<List<String>,List<Long>>>> metricValuesStore;

        protected ScopedRestrictedRuntimeContext(final RuntimeContext ctx, final MetricGroup scopedGroup, final AtomicReference<ListState<Map<List<String>,List<Long>>>> metricValuesStore){
            this.runtimeContext = ctx;
            this.scopedGroup = scopedGroup;
            this.metricValuesStore = metricValuesStore;
        }


        @Override
        public MetricGroup getMetricGroup() {
            return this.scopedGroup;
        }

        @Override
        public String getTaskName() {
            return runtimeContext.getTaskName();
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return runtimeContext.getNumberOfParallelSubtasks();
        }

        @Override
        public int getMaxNumberOfParallelSubtasks() {
            return runtimeContext.getMaxNumberOfParallelSubtasks();
        }

        @Override
        public int getIndexOfThisSubtask() {
            return runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public int getAttemptNumber() {
            return runtimeContext.getAttemptNumber();
        }

        @Override
        public String getTaskNameWithSubtasks() {
            return runtimeContext.getTaskNameWithSubtasks();
        }

        @Override
        public ExecutionConfig getExecutionConfig() {
            return runtimeContext.getExecutionConfig();
        }

        @Override
        public ClassLoader getUserCodeClassLoader() {
            return runtimeContext.getUserCodeClassLoader();
        }

        // -----------------------------------------------------------------------------------
        // Unsupported operations
        // -----------------------------------------------------------------------------------

        @Override
        public DistributedCache getDistributedCache() {
            throw new UnsupportedOperationException("Distributed cache is not supported in rich async functions.");
        }

        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException("State is not supported in rich async functions.");
        }

        @Override
        public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
            ListState result = Match((ListStateDescriptor) stateProperties).of(
                    Case($(is(MetricUtils.METRIC_VALUES_STATE)), (descriptor) -> metricValuesStore.get()),
                    Case($(), (x) -> {
                        throw new UnsupportedOperationException("State is not supported in rich async functions.");
                    })
            );
            return result;
        }

        @Override
        public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException("State is not supported in rich async functions.");
        }

        @Override
        public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
            throw new UnsupportedOperationException("State is not supported in rich async functions.");
        }

        @Override
        public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
            throw new UnsupportedOperationException("State is not supported in rich async functions.");
        }

        @Override
        public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
            throw new UnsupportedOperationException("State is not supported in rich async functions.");
        }

        @Override
        public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
            throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
        }

        @Override
        public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
            throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
        }

        @Override
        public java.util.Map<String, Accumulator<?, ?>> getAllAccumulators() {
            throw new UnsupportedOperationException("Accumulators are not supported in rich async functions.");
        }

        @Override
        public IntCounter getIntCounter(String name) {
            throw new UnsupportedOperationException("Int counters are not supported in rich async functions.");
        }

        @Override
        public LongCounter getLongCounter(String name) {
            throw new UnsupportedOperationException("Long counters are not supported in rich async functions.");
        }

        @Override
        public DoubleCounter getDoubleCounter(String name) {
            throw new UnsupportedOperationException("Long counters are not supported in rich async functions.");
        }

        @Override
        public Histogram getHistogram(String name) {
            throw new UnsupportedOperationException("Histograms are not supported in rich async functions.");
        }

        @Override
        public boolean hasBroadcastVariable(String name) {
            throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
        }

        @Override
        public <RT> List<RT> getBroadcastVariable(String name) {
            throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
        }

        @Override
        public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
            throw new UnsupportedOperationException("Broadcast variables are not supported in rich async functions.");
        }


    }

    protected static class ScopedRestrictedIterationRuntimeContext extends ScopedRestrictedRuntimeContext implements IterationRuntimeContext {

        private final IterationRuntimeContext iterationRuntimeContext;

        protected ScopedRestrictedIterationRuntimeContext(final IterationRuntimeContext iterationRuntimeContext, final MetricGroup scopedGroup, final AtomicReference<ListState<Map<List<String>,List<Long>>>> metricValuesStore) {
            super(iterationRuntimeContext,scopedGroup, metricValuesStore);

            this.iterationRuntimeContext = Preconditions.checkNotNull(iterationRuntimeContext);
        }

        @Override
        public int getSuperstepNumber() {
            return iterationRuntimeContext.getSuperstepNumber();
        }

        // -----------------------------------------------------------------------------------
        // Unsupported operations
        // -----------------------------------------------------------------------------------

        @Override
        public <T extends Aggregator<?>> T getIterationAggregator(String name) {
            throw new UnsupportedOperationException("Iteration aggregators are not supported in rich async functions.");
        }

        @Override
        public <T extends Value> T getPreviousIterationAggregate(String name) {
            throw new UnsupportedOperationException("Iteration aggregators are not supported in rich async functions.");
        }
    }

    /**
     * Common adapter for Flink functions (basic and rich) decorated with Flink metrics
     * Its children are implementing FlinkMeteredFunction interface
     * Should be extended with operator-specific functional interface logic implementations ('map', 'mapAsync', 'invoke' etc.)
     */
    protected abstract class MeteredFlinkOperatorAdapter extends AbstractRichFunction implements CheckpointedFunction {

        private transient CheckedRunnableRunnerOnce runner;
        private transient AtomicReference<ListState<Map<List<String>,List<Long>>>> meterValuesState;
        private transient ListState<Map<List<String>,List<Long>>> flinkCheckpointedStateHookup;


        private AtomicReference<ListState<Map<List<String>,List<Long>>>> getMeterValuesState(){
            if (meterValuesState == null){
                initMeterValuesState();
            }
            return meterValuesState;
        }

        synchronized private void initMeterValuesState(){
            meterValuesState = new AtomicReference<>();
        }

        synchronized private void initRunner(){
           runner = new CheckedRunnableRunnerOnce();
        }

        private CheckedRunnableRunnerOnce getRunner(){
            if (runner == null){
                initRunner();
            }
            return runner;
        }

        // needed to handle a variety of rich Flink operators differently
        protected  <U extends RichFunction> Optional<U> getInnerRichFunctionOptionalTyped(Class<U> clazz){
            return  API.<Function>Match(innerFunction).of(
                    Case($(instanceOf(clazz)), func -> Optional.of(func)),
                    Case($(), Optional.empty())
            );
        }

        private Optional<RichFunction> getInnerRichFunctionOptional(){
            return getInnerRichFunctionOptionalTyped(RichFunction.class);
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
            getInnerRichFunctionOptional().ifPresent(func -> func.setRuntimeContext(t));
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }

        @Override
        public void close() throws Exception {
            super.close();
            final CheckedRunnableRunnerOnce runner = getRunner();
            synchronized (runner) {
                getInnerRichFunctionOptional().ifPresent(func -> runner.accept(func::close));
                runner.run();
            }
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            final CheckedRunnableRunnerOnce runner = getRunner();
            synchronized (runner) {
                getInnerRichFunctionOptional().ifPresent(func -> runner.accept(() -> func.open(parameters)));
                runner.run();
            }
            RuntimeContext ctx = getRuntimeContext();
            MetricGroup mg = MetricUtils.addAtScope(ctx.getMetricGroup(), userMetricGroupScope);
            RuntimeContext scoped = Match(ctx).of(
                    Case($(instanceOf(IterationRuntimeContext.class)), c -> new ScopedRestrictedIterationRuntimeContext(c, mg, getMeterValuesState())),
                    Case($(), c -> new ScopedRestrictedRuntimeContext(c, mg, getMeterValuesState()))
            );
            metricsCalculators.all().forEach(entry -> entry.forEach(calc -> calc.bootstrap(scoped)));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            AtomicReference<io.vavr.collection.Map<List<String>, io.vavr.collection.Seq<Long>>> oldResult = new AtomicReference<>(io.vavr.collection.HashMap.empty());
            StreamSupport.stream(Spliterators.spliteratorUnknownSize(flinkCheckpointedStateHookup.get().iterator(), Spliterator.ORDERED),
                    false).forEach(e -> oldResult.set(oldResult.get().merge(io.vavr.collection.HashMap.ofAll(e)
                    .mapValues(v -> io.vavr.collection.Array.ofAll(v)), (a,b) -> a.appendAll(b))));
            flinkCheckpointedStateHookup.clear();
            AtomicReference<io.vavr.collection.Map<List<String>, io.vavr.collection.Seq<Long>>> newResult = new AtomicReference<>(io.vavr.collection.HashMap.empty());
            metricsCalculators.all().forEach(e -> newResult.set(newResult.get().merge(io.vavr.collection.HashMap.ofAll(e.getCombinedMetricValuesSnapshot())
                    .mapValues(v -> io.vavr.collection.Array.ofAll(v)), (a,b) -> a.appendAll(b))));

            io.vavr.collection.Map<List<String>, io.vavr.collection.Seq<Long>> finalResult = newResult.get().merge(oldResult.get(),  (a,b) -> a);
            flinkCheckpointedStateHookup.add(finalResult.mapValues((v)->v.toJavaList()).toJavaMap());
            logger.info(String.format("Checkpointing the snapshot of the current metric values state: %s", flinkCheckpointedStateHookup.get()));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            flinkCheckpointedStateHookup = useEvenSplitStateRedistribution ?
                context.getOperatorStateStore().getListState(MetricUtils.METRIC_VALUES_STATE) :
                context.getOperatorStateStore().getUnionListState(MetricUtils.METRIC_VALUES_STATE);
            if (context.isRestored()){
                logger.info(String.format("Restoring metric values state from the checkpointed snapshot %s", flinkCheckpointedStateHookup.get()));
                getMeterValuesState().set(flinkCheckpointedStateHookup);
            }
        }

    }

}
