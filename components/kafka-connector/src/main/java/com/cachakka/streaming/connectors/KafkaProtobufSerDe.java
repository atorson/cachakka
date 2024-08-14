package com.cachakka.streaming.connectors;

import com.cachakka.streaming.core.api.StreamingEntity;
import com.cachakka.streaming.core.api.StreamingEntityCompanion;
import com.cachakka.streaming.core.utils.ProtobufCompanionSupport;
import com.cachakka.streaming.core.utils.FlinkReflectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class KafkaProtobufSerDe<E extends StreamingEntity<?,E>, S, C extends KafkaProtobufSerDe<E,S,C>> implements KeyedDeserializationSchema<S>,KeyedSerializationSchema<S>,
        Serializer<S>, Deserializer<S> {

    private transient StreamingEntityCompanion<?,E> companion;
    private final TypeInformation<E> valueType;
    private final TypeInformation<S> serializedValueType;

    protected C reifySelf(){
        return (C) this;
    }

    public KafkaProtobufSerDe(){
        this.valueType = new FlinkReflectionUtils.ReflectionTypeExtractor<E>().extractTypeInformation(
                reifySelf(), KafkaProtobufSerDe.class, 0).get();
        this.serializedValueType = new FlinkReflectionUtils.ReflectionTypeExtractor<S>().extractTypeInformation(
                reifySelf(), KafkaProtobufSerDe.class, 1).get();
        initCompanion();
    }

    public abstract String extractKey(S data);

    public abstract E extractValue(S data);

    public abstract S create(String key, E value);

    private synchronized void initCompanion(){
        this.companion = ProtobufCompanionSupport.findStreamingEntityCompanion(getValueType().getTypeClass());
    }

    public StreamingEntityCompanion<?,E> getCompanion(){
        if (companion == null){
            initCompanion();
        }
        return companion;
    }

    public TypeInformation<E> getValueType() {
        return valueType;
    }

    @Override
    public S deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        return create(messageKey != null? new String(messageKey, StandardCharsets.UTF_8): null, getCompanion().parseFrom(message));
    }

    @Override
    public boolean isEndOfStream(S nextElement) {
        return false;
    }

    @Override
    public TypeInformation<S> getProducedType() {
        return serializedValueType;
    }

    @Override
    public byte[] serializeKey(S element) {
        String key = extractKey(element);
        return key != null? key.getBytes(): null;
    }

    @Override
    public byte[] serializeValue(S element) {
        return extractValue(element).toByteArray();
    }


    @Override
    public S deserialize(String topic, byte[] data) {
        return create(null, getCompanion().parseFrom(data));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (isKey) throw new IllegalArgumentException("KafkaProtobufSerDe must not be used for message keys");
    }

    @Override
    public byte[] serialize(String topic, S data) {
        return serializeValue(data);
    }

    @Override
    public void close() {}

    @Override
    public String getTargetTopic(S element) {return null;}
}
