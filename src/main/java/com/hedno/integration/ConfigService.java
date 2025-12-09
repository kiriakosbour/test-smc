package com.hedno.integration;

import java.io.InputStream;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton	
@Startup
public class ConfigService {

    private static final Logger log = LoggerFactory.getLogger(ConfigService.class);
    private static final String PROPERTY_PATH = "config/application.properties";
    private static boolean loaded = false;
    
    private static final Properties properties = new Properties();

    @PostConstruct
    public void init() {
        try (InputStream input = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(PROPERTY_PATH)) {

            if (input == null) {
                throw new IllegalStateException("Cannot find " + PROPERTY_PATH);
            }

            properties.load(input);
            loaded = true;
            log.info(">>> Loaded application.properties from WAR (" + properties.size() + " properties)");
        } catch (Exception e) {
        	log.error("Error loading properties", e);
            throw new RuntimeException("Error loading properties", e);
        }
    }

    public static String get(String key) {
        if (!loaded) {
            // if called before @PostConstruct (startup race)
            synchronized (ConfigService.class) {
                if (!loaded) {
                    new ConfigService().init();
                }
            }
        }
        return properties.getProperty(key);
    }
    
    
    public static String get(String key, String defaultValue) {
    	String value = get(key);
    	return value != null ? value : defaultValue;
    }    
}	