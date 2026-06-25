package com.pigdawg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class ConfigLoader {
    private static final String APPLICATION_PROPERTIES = "application.properties";

    private ConfigLoader() {
    }

    static Properties loadApplicationProperties(Class<?> anchorClass) {
        try (InputStream inputStream = anchorClass.getClassLoader().getResourceAsStream(APPLICATION_PROPERTIES)) {
            if (inputStream == null) {
                throw new IllegalStateException("Unable to find " + APPLICATION_PROPERTIES + " on the classpath");
            }

            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    static String getRequiredProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }

        return value;
    }
}
