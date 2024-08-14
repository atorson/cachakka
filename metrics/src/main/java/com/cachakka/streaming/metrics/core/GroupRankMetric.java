package com.cachakka.streaming.metrics.core;

import org.apache.flink.metrics.Metric;

public interface GroupRankMetric extends Metric {

    String getGroupRankName();

    String getGroupMemberId();

    String getGroupMemberName();

}
