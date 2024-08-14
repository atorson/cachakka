package com.cachakka.streaming.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public abstract class AbstractConfigurationProvider implements ConfigurationProvider{

    private static final Logger logger = LoggerFactory.getLogger(AbstractConfigurationProvider.class);

    protected static Config loadConfig(Class configProviderClazz, String configResourceName, boolean loadReferenceConfig) {
        Config config;
        config = innerLoadConfig(Thread.currentThread().getContextClassLoader(), configResourceName, loadReferenceConfig,
                () -> "CurrentThread");

        if (config == null) {
            config = innerLoadConfig(configProviderClazz.getClassLoader(), configResourceName, loadReferenceConfig,
                    () -> configProviderClazz.getName());
        }

        if (config == null) {
            StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
            for (int i = 1; i < stElements.length; i++) {
                StackTraceElement ste = stElements[i];
                if (!ste.getClass().equals(LocalConfigurationProvider.class)
                        && ste.getClassName().indexOf("java.lang.Thread") != 0) {
                    config = innerLoadConfig(ste.getClass().getClassLoader(), configResourceName, loadReferenceConfig,
                            () -> ste.getClassName());
                    if (config != null) {
                        break;
                    }
                }
            }
        }

        if (config != null) {
            logger.info(String.format("Application configuration loaded from %s: %s",
                    configResourceName, config.root().render(ConfigRenderOptions.concise())));
            return config;

        } else {
            throw new ConfigException.Generic(String.format("Application configuration could not be loaded from %s",
                    configResourceName) + (loadReferenceConfig? " including reference configuration": ""));
        }

    }

    protected static Config innerLoadConfig(ClassLoader cl, String configResourceName,
                                            boolean loadReferenceConfig, Supplier<String> onErrorOrigin) {
        Config config;
        try {
            config = ConfigFactory.parseResourcesAnySyntax(cl, configResourceName);
            if (loadReferenceConfig) {
                config = config.withFallback(ConfigFactory.defaultReference(cl)).resolve();
            }
            return config;
        } catch (Exception e) {
            logger.warn(String.format("Failed to load configuration %s using classloader %s of the caller %s with parent loader %s",
                        configResourceName,
                        cl != null ? cl.getClass().getName() : null, onErrorOrigin.get(),
                        cl != null && cl.getParent() != null ? cl.getParent().getClass().getName() : null), e);
            return null;
        }
    }
}
