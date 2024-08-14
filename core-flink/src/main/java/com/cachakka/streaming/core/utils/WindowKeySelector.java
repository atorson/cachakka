package com.cachakka.streaming.core.utils;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * This key selector keys a subtask data to the same subtask itself.
 * We do this because Flink doesn't support subtask level parallel windowing. The only way to achieve parallel windowing is using a KeyedStream.
 * So we need to convert a DataStream into a KeyedStream, while retaining the data distribution (and hence avoiding skewness) among different subtasks.
 * @param <T>
 */
public class WindowKeySelector<T> implements KeySelector<Tuple3<T, Integer, Integer>, Integer> {

    @Override
    public Integer getKey(Tuple3<T, Integer, Integer> value) throws Exception {

        Integer operatorIndex = value.f1;
        Integer operatorParallelism = value.f2;

        Integer defaultMaxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(operatorParallelism);
        KeyGroupRange kgr = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(defaultMaxParallelism, operatorParallelism, operatorIndex);

        Integer i = 1;

        // Multiplier of 3 was chosen based on experiments
        for (; i < operatorParallelism * 3; i++) {
            if(kgr.contains(KeyGroupRangeAssignment.computeKeyGroupForKeyHash(i, defaultMaxParallelism))) {
                break;
            }
        }
        return i;
    }
}
