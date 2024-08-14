package com.cachakka.streaming.metrics.core;


import com.cachakka.streaming.core.api.StreamingFlowDataTypeAware;
import com.cachakka.streaming.metrics.calculator.CompositeFlinkMetricsCalculator;
import io.vavr.collection.Seq;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;



/**
 *  type-safe interface to hold many different metric calculators indexed by their streaming data type
 */
public interface FlinkMetricCalculatorsRegistry extends Serializable{

    /**
     * type-safe interface to hold collection of similar-type metric calculators
     * isolation & ordering of calculators is important: reflects the param structure of the wrapped/underlying Flink operator
     * most common case: just one metric calculator is present in the entry
     * @param <T> streaming data type of the metrics calculator
     */
    interface Entry<T> extends StreamingFlowDataTypeAware<T>, Serializable{

        /**
         * Add operator
         * @param calculator calculator
         */
        void add(FlinkMetricsCalculator<T> calculator);

        /**
         * List-like get operator
         * @param index index
         * @return result, if available
         * @throws IndexOutOfBoundsException exception
         */
        @Nullable FlinkMetricsCalculator<T> get(int index) throws IndexOutOfBoundsException;

        /**
         * Collection-like operator forEach()
         * @param consumer function
         */
        void forEach(Consumer<FlinkMetricsCalculator<T>> consumer);

        /**
         * Collection-like operator size()
         * @return size
         */
        int size();

        /**
         * Scala-style sequence get operation
         * @param index index to get at
         * @return element, if present
         */
        default Optional<FlinkMetricsCalculator<T>> getOptional(int index){
            try {
                return Optional.of(get(index));
             } catch (Exception e) {
                return Optional.empty();
            }
        }

        default Map<List<String>, List<Long>> getCombinedMetricValuesSnapshot() {
            AtomicReference<io.vavr.collection.Map<List<String>, Seq<Long>>> result = new AtomicReference<>(io.vavr.collection.HashMap.empty());
            this.forEach(c -> result.set(result.get().merge(io.vavr.collection.HashMap.ofAll(c.getMetricValuesSnapshot())
                            .mapValues(v -> io.vavr.collection.Array.ofAll(v)), (a,b) -> a.appendAll(b))));
            return result.get().mapValues((v)->v.toJavaList()).toJavaMap();
        }

        default List<Long> recoverCombinedMeterHistory(List<String> scope) {
            final List<Long> result = new ArrayList<>();
            this.forEach(c -> result.addAll(c.recoverMeterHistory(scope)));
            return result;
        }
    }

    /**
     * Retrieves an entry
     * Strongly-typed
     * @param key class of the streaming data type of the metrics calculators (because Java does not generate & infer it from T)
     * @param <T> streaming data type
     * @return matching registered entry, if found
     */
    <T> Optional<Entry<T>> retrieve(@Nonnull TypeInformation<T> key);

    /**
     * Retrieves all entries (not strongly-typed)
     * @return all entries
     */
    Collection<Entry<?>> all();

    /**
     * Fluent builder API
     * @param calculator metrics calculator
     * @param <T> streaming data type
     * @return self
     */
    <T> FlinkMetricCalculatorsRegistry withCalculator(@Nonnull final FlinkMetricsCalculator<T> calculator);

    /**
     * Handy version of the 'withCalculator
     * @param calculator calculator option
     * @param <T> streaming data type
     * @return self
     */
    default <T> FlinkMetricCalculatorsRegistry withCalculatorOption(final Optional<FlinkMetricsCalculator<T>> calculator){
       calculator.ifPresent(this::withCalculator);
       return this;
    }

    /**
     * Factory create() method
     * @return new registry instance
     */
    static FlinkMetricCalculatorsRegistry newInstance(){
        return new FlinkMetricCalculatorsRegistry(){
            ConcurrentHashMap<TypeInformation<?>, Entry<?>> innerMap = new ConcurrentHashMap<>();

            @Override
            public <T> Optional<Entry<T>> retrieve(TypeInformation<T> key) {
                // safe casting here because of the design of add() operations in this registry
                // howerver, in Scala it can be done without cast using its match/case SDK facility
                return Optional.ofNullable((Entry<T>) innerMap.get(key));
            }


            @Override
            public Collection<Entry<?>> all() {
                return innerMap.values();
            }

            /**
             * Creates new empty entry
             * @param typeInfo streaming data type class
             * @param <T>
             * @return new entry
             */
            private <T> Entry<T> newEntry(@Nonnull TypeInformation<T> typeInfo){
                return new CompositeFlinkMetricsCalculator<T>().withTypeInfo(typeInfo);
            }

            @Override
            public <T> FlinkMetricCalculatorsRegistry withCalculator(FlinkMetricsCalculator<T> calculator) {
                // safe casting here because of the design of add() operations in this registry
                // howerver, in Scala it can be done without cast using its match/case SDK facility
                ((Entry<T>) innerMap.computeIfAbsent(calculator.getProducedType(), (key) -> newEntry(key))).add(calculator);
                return this;
            }
        };
    }


}
