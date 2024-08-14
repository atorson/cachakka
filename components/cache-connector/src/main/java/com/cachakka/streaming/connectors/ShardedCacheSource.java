package com.cachakka.streaming.connectors;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.PoisonPill$;
import akka.util.BoundedBlockingQueue;
import com.typesafe.config.Config;
import com.cachakka.streaming.akka.AkkaCluster;
import com.cachakka.streaming.akka.shard.cache.CacheConfig;
import com.cachakka.streaming.akka.shard.cache.CacheConsumer;
import com.cachakka.streaming.akka.shard.cache.CacheShardExtension;
import com.cachakka.streaming.akka.shard.cdc.*;
import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import io.vavr.Function0;
import io.vavr.control.Try;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Flink-based wrapper around CacheConsumer implementing Source interface
 * Note: is using blocking queue to propagate backpressure through the consumer's drain thread
 * @param <E>
 */
public class ShardedCacheSource<E extends CDCEntity> extends RichParallelSourceFunction<E> implements PartitionedSourceFunction<E, Map<String, Supplier<? extends Serializable>>> {

    private final TypeInformation<E> valueType;
    private final double rateMultiplier;
    private final String groupId;
    private final Set<String> agentFilterInclude;
    private final Set<String> agentFilterExclude;
    private final AtomicReference<Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>>> callback;
    private final AtomicBoolean isRunning;
    private transient ActorRef consumer;
    private transient Map<String,CDCEnum> agentEnums;
    private transient BlockingQueue<List<E>> queue;

    public ShardedCacheSource(final String groupId, final double rateMultiplier){
        this(groupId, rateMultiplier, null, null);
    }

    public ShardedCacheSource(final String groupId, final double rateMultiplier, Set<? extends CDCAgent> agentFilter, boolean isFilterInclude){
        this(groupId, rateMultiplier, isFilterInclude? agentFilter: null, isFilterInclude? null: agentFilter);
    }

    public ShardedCacheSource(final String groupId, final double rateMultiplier, Set<? extends CDCAgent> agentFilterInclude, Set<? extends CDCAgent> agentFilterExclude){
        this.valueType = new FlinkReflectionUtils.ReflectionTypeExtractor<E>().extractTypeInformation(
                this, ShardedCacheSource.class, 0).get();
        isRunning = new AtomicBoolean(false);
        callback = new AtomicReference<>();
        this.rateMultiplier = rateMultiplier;
        this.groupId = groupId;
        this.agentFilterInclude = agentFilterInclude == null? null: agentFilterInclude.stream().map(s -> s.name()).collect(Collectors.toSet());
        this.agentFilterExclude = agentFilterExclude == null? null: agentFilterExclude.stream().map(s -> s.name()).collect(Collectors.toSet());
    }

    @Override
    public void onPartitionAssignment(Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>> callback) {
        this.callback.set(callback);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public TypeInformation<E> getProducedType() {
        return valueType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        AkkaCluster.blockUntilMemeberUp();
    }

    @Override
    public void run(SourceContext<E> sc) throws Exception {
        agentEnums = scala.collection.JavaConverters.mapAsJavaMapConverter(CDCShardExtension.get(AkkaCluster.actorSystem()).cassContext().enums()).asJava();
        isRunning.compareAndSet(false,true);
        RuntimeContext ctx = getRuntimeContext();
        queue = new ArrayBlockingQueue<>(1);
        consumer = AkkaCluster.actorSystem().actorOf(CacheConsumer.propsJava(valueType.getTypeClass(), groupId,
                ctx.getNumberOfParallelSubtasks(), ctx.getIndexOfThisSubtask(), rateMultiplier, new HashMap<>(), (c) -> {try { queue.put(c);} catch (InterruptedException e1) {} },
                agentFilterInclude == null? null: agentFilterInclude.stream().map((e) -> (CDCAgent) agentEnums.get(e)).collect(Collectors.toSet()),
                agentFilterExclude == null? null: agentFilterExclude.stream().map((e) -> (CDCAgent) agentEnums.get(e)).collect(Collectors.toSet())),
                String.format("cache-consumer-%s-%s-%s", groupId, ctx.getIndexOfThisSubtask(), new Random().nextInt(1000000)));
        while(isRunning.get()){
            try {queue.take().forEach(e -> sc.collect(e));} catch (InterruptedException e) {}
        }
        queue.clear();
    }

    @Override
    public void cancel() {
       if (consumer != null) {
           consumer.tell(PoisonPill$.MODULE$, null);
       }
       isRunning.compareAndSet(true, false);
    }
}
