package com.cachakka.streaming.connectors;

import com.google.common.collect.ImmutableMap;
import com.cachakka.streaming.core.api.PartitionedSourceFunction;
import io.vavr.Function0;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerCallBridge;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerCallBridge010;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KafkaSource<T> extends FlinkKafkaConsumer010<T> implements PartitionedSourceFunction<T,Map<String, Supplier<? extends Serializable>>> {

    protected final AtomicReference<Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>>> callback;

    public KafkaSource(List<String> topics, KeyedDeserializationSchema<T> schema, Properties props) {
        super(topics, schema, props);
        callback = new AtomicReference<>();
    }

    @Override
    public void onPartitionAssignment(Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>> callback) {
        this.callback.set(callback);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
            SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {

        // make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
        // this overwrites whatever setting the user configured in the properties
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS || offsetCommitMode == OffsetCommitMode.DISABLED) {
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        OverridenKafka10Fetcher<T> result =
                new OverridenKafka10Fetcher<T>(
                        callback,
                        sourceContext,
                        assignedPartitionsWithInitialOffsets,
                        watermarksPeriodic,
                        watermarksPunctuated,
                        runtimeContext.getProcessingTimeService(),
                        runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                        runtimeContext.getUserCodeClassLoader(),
                        runtimeContext.getTaskNameWithSubtasks(),
                        deserializer,
                        properties,
                        pollTimeout,
                        runtimeContext.getMetricGroup(),
                        consumerMetricGroup,
                        useMetrics);

        return result;
    }

    private static class OverridenKafka10Fetcher<T> extends Kafka010Fetcher<T> {

        private final AtomicReference<Map<MetricName, ? extends Metric>> kafkaMetrics;
        private final AtomicReference<Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>>> callback;

        private class OverridenKafkaConsumerCallBridge extends KafkaConsumerCallBridge010{
        @Override
            public void assignPartitions(KafkaConsumer<?, ?> consumer, List<TopicPartition> topicPartitions) throws Exception {
                super.assignPartitions(consumer, topicPartitions);
                // hookup to get Kafka consumer metrics
                kafkaMetrics.set(consumer.metrics());
            }
        }

        private OverridenKafka10Fetcher(
                AtomicReference<Function0<Consumer<List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>>>>> callback,
                SourceContext<T> sourceContext,
                Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
                SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
                SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
                ProcessingTimeService processingTimeProvider,
                long autoWatermarkInterval,
                ClassLoader userCodeClassLoader,
                String taskNameWithSubtasks,
                KeyedDeserializationSchema<T> deserializer,
                Properties kafkaProperties,
                long pollTimeout,
                MetricGroup subtaskMetricGroup,
                MetricGroup consumerMetricGroup,
                boolean useMetrics) throws Exception {
            super(
                    sourceContext,
                    assignedPartitionsWithInitialOffsets,
                    watermarksPeriodic,
                    watermarksPunctuated,
                    processingTimeProvider,
                    autoWatermarkInterval,
                    userCodeClassLoader,
                    taskNameWithSubtasks,
                    deserializer,
                    kafkaProperties,
                    pollTimeout,
                    subtaskMetricGroup,
                    consumerMetricGroup,
                    useMetrics);
            kafkaMetrics = new AtomicReference<>();
            this.callback = callback;
            // call the callback immediately since there are subscribed partition states available when Fetcher is created
            Optional.ofNullable(this.callback.get()).ifPresent(c -> c.get().accept(getSubscribedPartitionViews()));
        }


        protected final List<SourcePartitionState<Map<String, Supplier<? extends Serializable>>>> getSubscribedPartitionViews() {
            return this.subscribedPartitionStates().stream().map(p -> new KafkaSourcePartitionStateView(p,
                    () -> getKafkaConsumerLagMetric(p))).collect(Collectors.toList());
        }

        private Optional<Metric> getKafkaConsumerLagMetric(final KafkaTopicPartitionState<TopicPartition> p) {
          return Optional.ofNullable(kafkaMetrics.get()).flatMap(m -> m.entrySet().stream()
                  .filter(e -> e.getKey().name().endsWith(partitionLagMetricName(p.getKafkaPartitionHandle())))
                  .findAny().map(e -> e.getValue()));
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }

        @Override
        public void addDiscoveredPartitions(List<KafkaTopicPartition> newPartitions) throws IOException, ClassNotFoundException {
            super.addDiscoveredPartitions(newPartitions);
            // call the callback because there are new partitions discovered
            Optional.ofNullable(callback.get()).ifPresent(c -> c.get().accept(getSubscribedPartitionViews()));
        }

        @Override
        protected KafkaConsumerCallBridge010 createCallBridge() {
            return new OverridenKafkaConsumerCallBridge();
        }
    }


    private static class KafkaSourcePartitionStateView implements SourcePartitionState<Map<String, Supplier<? extends Serializable>>>{

        private final KafkaTopicPartitionState<?> kafkaTopicPartitionState;
        private final Supplier<Optional<Metric>> kafkaConsumerLagMetric;

        private KafkaSourcePartitionStateView(final KafkaTopicPartitionState<?> kafkaTopicPartitionState, final Supplier<Optional<Metric>> kafkaConsumerLagMetric){
            this.kafkaTopicPartitionState = kafkaTopicPartitionState;
            this.kafkaConsumerLagMetric = kafkaConsumerLagMetric;
        }

        @Override
        public List<String> getPartitionKey() {
            return Arrays.asList("PartitionState", kafkaTopicPartitionState.getTopic(), new Integer(kafkaTopicPartitionState.getPartition()).toString());
        }

        @Override
        public Map<String, Supplier<? extends Serializable>> get() {
            return ImmutableMap.<String, Supplier<? extends Serializable>>builder()
                    .put(new AbstractMap.SimpleEntry<>("CommittedOffset", () -> kafkaTopicPartitionState.getCommittedOffset() >=0?
                            kafkaTopicPartitionState.getCommittedOffset(): Math.max(0L, kafkaTopicPartitionState.getOffset() + 1)))
                    .put(new AbstractMap.SimpleEntry<>("ConsumerLag", () -> kafkaConsumerLagMetric.get().map(m -> m.value()).orElse(null)))
                    .put(new AbstractMap.SimpleEntry<>("LatestOffset", () -> kafkaConsumerLagMetric.get().map(m ->
                            Math.max(0L,kafkaTopicPartitionState.getOffset() + 1 + m.value())).orElse(null)))
                    .put(new AbstractMap.SimpleEntry<>("ProcessingOffset", () -> Math.max(0L, kafkaTopicPartitionState.getOffset() + 1)))
                    .put(new AbstractMap.SimpleEntry<>("BufferedLag", () -> Math.max(0L, kafkaTopicPartitionState.getOffset() + 1 -
                            (kafkaTopicPartitionState.getCommittedOffset() >=0?
                                    kafkaTopicPartitionState.getCommittedOffset(): Math.max(0L, kafkaTopicPartitionState.getOffset() + 1)))))
                    .build();
        }
    }

}
