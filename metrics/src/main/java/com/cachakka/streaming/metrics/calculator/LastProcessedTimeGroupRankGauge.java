package com.cachakka.streaming.metrics.calculator;

import com.cachakka.streaming.metrics.core.GroupRankMetric;
import org.apache.flink.metrics.Gauge;

import java.util.concurrent.atomic.AtomicReference;

public class LastProcessedTimeGroupRankGauge implements Gauge<Long>, GroupRankMetric {


    private final String groupRankName;

    private final String groupMemberId;

    private final String groupMemberName;

    private final AtomicReference<Long> lastTimeRef;

    public LastProcessedTimeGroupRankGauge(String groupRankName, String groupMemberId, String groupMemberName, AtomicReference<Long> lastTimeRef) {
        this.groupRankName = groupRankName;
        this.groupMemberId = groupMemberId;
        this.groupMemberName = groupMemberName;
        this.lastTimeRef = lastTimeRef;
    }

    @Override
    public String getGroupRankName() {
        return groupRankName;
    }

    @Override
    public String getGroupMemberId() {
        return groupMemberId;
    }

    @Override
    public String getGroupMemberName() {
        return groupMemberName;
    }

    @Override
    public Long getValue() {
        return lastTimeRef.get();
    }

    public AtomicReference<Long> getLastTimeRef() {
        return lastTimeRef;
    }
}
