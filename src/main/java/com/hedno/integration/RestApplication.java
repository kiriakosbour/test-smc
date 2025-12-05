package com.hedno.integration;

import com.hedno.integration.controller.MdmPushController;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * JAX-RS Application configuration.
 * Registers REST endpoints.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
@ApplicationPath("/api")
public class RestApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<>();
        classes.add(MdmPushController.class);
        return classes;
    }
}
