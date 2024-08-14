package com.cachakka.streaming.core.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Typed marker interface to overcome Java's lack of elegant type erasure facilities (like ClassTag[] compile-time generation in Scala)
 * @param <T>
 */
public interface StreamingFlowDataTypeAware<T> extends ResultTypeQueryable<T>{

    /**
     * Provides streaming data type info for strongly-typed generic classes
     * @return class of streaming data type
     *
     */
    TypeInformation<T> getStreamingDataType();

    @Override
    default TypeInformation<T> getProducedType() {
        return getStreamingDataType();
    }

}
