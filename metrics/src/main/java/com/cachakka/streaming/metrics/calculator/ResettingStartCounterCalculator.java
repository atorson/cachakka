package com.cachakka.streaming.metrics.calculator;

import java.util.List;

/**
 * Increments every time there is a recovery (on restart)
 * Has persistent counter state
 * Does not increment on events
 * @param <T>
 */
public class ResettingStartCounterCalculator<T> extends ResettingEventCounterCalculator<T> {

    public ResettingStartCounterCalculator(){
        super();
        this.evaluator = (x,s) -> null;
    }


    @Override
    public List<Long> recoverMeterHistory(List<String> scope) {
        return io.vavr.collection.Array.ofAll(super.recoverMeterHistory(scope)).append(1L).toJavaList();
    }

}
