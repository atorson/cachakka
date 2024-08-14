package com.cachakka.streaming.metrics.core;

import com.cachakka.streaming.core.utils.Utils;
import org.apache.flink.metrics.Counter;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;


/**
 * Simple, non-thread-safe implementation of resetable counter
 * Ok to be used within Flink function wrappers
 */
public class ResettingCounter implements Counter {

    private Long count;
    private Long resetAt;
    private int resetInterval;

    public ResettingCounter(int resetInterval){
        DateTime now = Utils.currentTime();
        this.resetInterval = resetInterval;
        this.resetAt = getNextResetDate(now);
        this.count = 0L;
    }

    private boolean resetIfNeeded(){
       DateTime now = Utils.currentTime();
       if (now.getMillis() - resetAt > 0){
           resetAt = getNextResetDate(now);
           count = 0L;
           return true;
       }
       return false;
    }

    @Nonnull
    public Long getNextScheduledResetTime() {
        return resetAt;
    }

    private Long getNextResetDate(DateTime now){
        return now.withTimeAtStartOfDay().plusDays(resetInterval).withTimeAtStartOfDay().getMillis();
    }

    @Override
    public void inc() {
        resetIfNeeded();
        count++;
    }

    @Override
    public void inc(long n) {
        resetIfNeeded();
        count += n;
    }

    @Override
    public void dec() {
        resetIfNeeded();
        count--;
    }

    @Override
    public void dec(long n) {
        resetIfNeeded();
        count -= n;
    }

    @Override
    public long getCount() {
        resetIfNeeded();
        return count;
    }


}
