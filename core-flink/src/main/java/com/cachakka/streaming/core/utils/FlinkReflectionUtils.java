package com.cachakka.streaming.core.utils;


import com.google.common.base.Preconditions;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Optional;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;


public class FlinkReflectionUtils {

    /**
     * Provides a two-step type extractor & validator
     * First step is to construct it (at this point type hint may be possible to generate & capture at compile-time)
     * Second step is to try to infer data type from a generic interface
     * If at least one step succeeds - then the result will be produced
     * if both succeed - the result would only be produced if the types match
     * @param <T> data type to infer
     */
    public static class ReflectionTypeExtractor<T> implements Serializable{

        private final Option<TypeInformation<T>> typeHint;

        public  ReflectionTypeExtractor(){
            typeHint = Option.ofOptional(Optional.ofNullable(Try.of(() -> TypeInformation.of(new TypeHint<T>(){})).getOrElse((TypeInformation<T>) null)));
        }

        public ReflectionTypeExtractor(@Nonnull TypeInformation<T> typeHint){
            this.typeHint = Option.of(typeHint);
        }

        /**
         * Extract & validate data type for a given generic operator and a given input argument's position
         *
         * @param <U>              type of interface
         * @param <C>              concrete sub-class implementing interface
         * @param concreteInstance concrete instance extending a strongly-typed generic interface (Java will supply meta-data for reflection then)
         * @param genericInterface strongly-typed generic interface
         * @return type info for the input argument at the given position
         */
        public <U, C extends U> Try<TypeInformation<T>> extractTypeInformation(C concreteInstance, Class<U> genericInterface, int position){
            return Try.<TypeInformation<T>>of(() -> Match(concreteInstance).<TypeInformation<T>>of(
                        Case($(instanceOf(ResultTypeQueryable.class).and((c) -> c.getProducedType() != null)), c -> c.getProducedType()),
                        Case($(), () -> TypeExtractor.createTypeInfo(genericInterface, concreteInstance.getClass(), position, null, null))
                    ))
                    .recoverWith((exc) -> Try.of(() -> typeHint.get()))
                    .map((t) -> new Tuple2<>(t, typeHint.getOrElse(t)))
                    .flatMap((tuple) -> Try.of(() -> {
                        if (!tuple._1.equals(tuple._2)) {
                            throw new IllegalStateException(getInputArgValidationErrorMsg(concreteInstance, genericInterface, tuple));
                        }
                        // try to produce a class
                        Preconditions.checkState(tuple._1.getTypeClass() != null && tuple._2.getTypeClass() != null,
                                getInputArgValidationErrorMsg(concreteInstance, genericInterface, tuple));
                        return tuple._1;
                    }));
        }

        private  <U, C extends U> String getInputArgValidationErrorMsg(C concreteInstance, Class<U> genericInterface, Tuple2<TypeInformation<T>,TypeInformation<T>> tuple){
            return String.format("Input argument type validation failed for instance %s of %s : mismatch of extracted types %s and %s",
                    concreteInstance, genericInterface, tuple._1, tuple._2);
        }
    }



}
