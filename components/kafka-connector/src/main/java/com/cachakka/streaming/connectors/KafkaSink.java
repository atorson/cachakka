package com.cachakka.streaming.connectors;

import com.cachakka.streaming.core.utils.Utils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.Serializable;
import java.util.Properties;
import java.util.Random;

public class KafkaSink<E> extends FlinkKafkaProducer010<E> {

    public KafkaSink(String topicName, KeyedSerializationSchema<E> schema, Properties properties){
        super(topicName, schema, properties, new KafkaUniformOutputPartitioner<>());
    }


    private static final class KafkaUniformOutputPartitioner<E1> extends FlinkKafkaPartitioner<E1> implements Serializable {
        @Override
        public int partition(E1 record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return (int) (Utils.toPositive(Utils.murmur((key != null && key.length > 0)? key: generateRandomKey())) % (long) partitions.length);
        }
    }

    private static byte[] generateRandomKey(){
        Random random = new Random();
        final byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return buffer;
    }
}