package com.cachakka.streaming.core.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Typed marker interface to overcome Java's lack of elegant type erasure facilities (like ClassTag[] compile-time generation in Scala)
 * @param <IN> input data type
 * @param <OUT> output data type
 */
public interface StreamingFunctionDataTypesAware<IN,OUT> extends StreamingFlowDataTypeAware<OUT>{

    /**
     * Provides streaming data type info for strongly-typed generic classes
     * @return class of streaming data type of the input
     *
     */
    TypeInformation<IN> getInputDataType();

    /**
     * Provides streaming data type info for strongly-typed generic classes
     * @return class of streaming data type of the output
     *
     */
    TypeInformation<OUT> getOutputDataType();

    @Override
    default TypeInformation<OUT> getStreamingDataType(){
        return getOutputDataType();
    }

}
