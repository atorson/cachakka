package com.cachakka.streaming.configuration;

import com.typesafe.config.Config;

public interface ConfigurationProvider {

    static ConfigurationProvider getInstance(){
        return LocalConfigurationProvider.getInstance();
    }

    Config getConfig();
}
