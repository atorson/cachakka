package com.cachakka.streaming.core.utils;

import com.cachakka.streaming.core.api.StreamingFlowDataTypeAware;
import com.cachakka.streaming.core.api.StreamingFunctionDataTypesAware;
import io.vavr.CheckedFunction1;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

public class FlinkUtils {

    public interface FilterLambda<T> extends FilterFunction<T>, StreamingFlowDataTypeAware<T>, Serializable {

        static <T> FilterLambda<T> of(final CheckedFunction1<T, Boolean> filter, final TypeInformation<T> typeInfo){
            return new FilterLambda<T>(){

                @Override
                public boolean filter(T value) throws Exception {
                    try {
                        return filter.apply(value);
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }

                @Override
                public TypeInformation<T> getStreamingDataType() {
                    return typeInfo;
                }
            };
        }
    }

    public interface MapLambda<IN,OUT> extends MapFunction<IN,OUT>, StreamingFunctionDataTypesAware<IN,OUT>, Serializable{

        static <IN,OUT> MapLambda<IN,OUT> of(final CheckedFunction1<IN,OUT> map, final TypeInformation<IN> inputTypeInfo, TypeInformation<OUT> outputTypeInfo){
            return new MapLambda<IN,OUT>(){

                @Override
                public OUT map(IN value) throws Exception{
                    try {
                        return map.apply(value);
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }

                @Override
                public TypeInformation<IN> getInputDataType() {
                    return inputTypeInfo;
                }

                @Override
                public TypeInformation<OUT> getOutputDataType() {
                    return outputTypeInfo;
                }
            };
        }
    }

    public interface FlatMapLambda<IN,OUT> extends FlatMapFunction<IN,OUT>, StreamingFunctionDataTypesAware<IN,OUT>, Serializable{

        static <IN,OUT> FlatMapLambda<IN,OUT> of(final CheckedFunction1<IN, List<OUT>> map, final TypeInformation<IN> inputTypeInfo, TypeInformation<OUT> outputTypeInfo){
            return new FlatMapLambda<IN,OUT>(){

                @Override
                public void flatMap(IN value, Collector<OUT> out) throws Exception {
                    try {
                        map.apply(value).forEach(out::collect);
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }

                @Override
                public TypeInformation<IN> getInputDataType() {
                    return inputTypeInfo;
                }

                @Override
                public TypeInformation<OUT> getOutputDataType() {
                    return outputTypeInfo;
                }
            };
        }
    }
}
