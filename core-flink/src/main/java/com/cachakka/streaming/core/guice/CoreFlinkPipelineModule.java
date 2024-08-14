package com.cachakka.streaming.core.guice;

import com.google.inject.Exposed;
import com.google.inject.Guice;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;

import java.io.IOException;

public class CoreFlinkPipelineModule extends PrivateModule {

    private final String checkPointDir;
    private final ParameterTool parameterTool;

    public CoreFlinkPipelineModule(ParameterTool parameterTool, String checkPointDir){
        this.checkPointDir = checkPointDir;
        this.parameterTool = parameterTool;
    }

    @Provides
    @Exposed
    AbstractStateBackend getStateBackend(){
        try {
            return new RocksDBStateBackend(checkPointDir, Boolean.TRUE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void configure() {
        bind(ParameterTool.class).toInstance(parameterTool);
        expose(ParameterTool.class);
    }
}
