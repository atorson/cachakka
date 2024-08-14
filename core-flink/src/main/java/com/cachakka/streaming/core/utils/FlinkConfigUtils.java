/**
 * 
 */
package com.cachakka.streaming.core.utils;

import com.google.common.base.Strings;
import com.typesafe.config.Config;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkConfigUtils {

	public static ParameterTool getFromConfig(Config config) {
		Properties properties = toProperties(config);
		return getFromProperties(properties);
	}

	public static Properties toProperties(Config config) {
		Properties properties = new Properties();
		config.entrySet().stream()
                .filter(e -> !Strings.isNullOrEmpty(e.getKey())
                        && e.getValue().unwrapped() != null
                        && !Strings.isNullOrEmpty(e.getValue().unwrapped().toString()))
                .forEach(e -> properties.setProperty(e.getKey(), e.getValue().unwrapped().toString()));
		return properties;
	}

	public static ParameterTool getFromProperties(Properties properties) {
		Map<String, String> map = new HashMap(properties);
		return ParameterTool.fromMap(map);
	}

}
