package com.cachakka.streaming.configuration;

import com.google.common.base.Strings;
import com.typesafe.config.Config;


public final class LocalConfigurationProvider extends AbstractConfigurationProvider {

    private static transient LocalConfigurationProvider innerInstance;
    private static transient Config config;

    private LocalConfigurationProvider() {
        super();
    }


    public static synchronized LocalConfigurationProvider getInstance() {
        if (innerInstance != null) {
            return innerInstance;
        }

        /*
         * Initialize the global configuration.
         */
        String targetEnvironment = System.getProperty("targetEnvironment");
        if (Strings.isNullOrEmpty(targetEnvironment)){
            throw new RuntimeException("System property 'targetEnvironment' is not set");
        }
        String streamingConfigPath = "streaming-" + targetEnvironment + ".properties";
        config = AbstractConfigurationProvider.loadConfig(LocalConfigurationProvider.class, streamingConfigPath, false);
        innerInstance = new LocalConfigurationProvider();
        return innerInstance;
    }

    @Override
    public Config getConfig() {
        return config;
    }
}
