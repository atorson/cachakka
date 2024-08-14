package com.cachakka.streaming.core.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by kshah2 on 1/12/18.
 */
public interface StreamingPipelineProvider {
    StreamExecutionEnvironment getStreamExecutionEnv();
}
