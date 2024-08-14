package com.cachakka.streaming.configuration;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.typesafe.config.Config;

public class InjectableClasspathResourceConfigProvider extends AbstractConfigurationProvider{

    @Inject
    public InjectableClasspathResourceConfigProvider(@Named("configResourceName") String configResourceName,
                                                     @Named("loadReferenceConfig") Boolean loadReferenceConfig){
        super();
        this.configResourceName = configResourceName;
        this.loadReferenceConfig = loadReferenceConfig;
    }

    private final String configResourceName;
    private final boolean loadReferenceConfig;
    private transient Config config;

    @Override
    synchronized public Config getConfig() {
         if (config == null) {
             config = AbstractConfigurationProvider.loadConfig(getClass(),configResourceName, loadReferenceConfig);
         }
         return config;
    }
}
