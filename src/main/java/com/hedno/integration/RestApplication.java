package com.hedno.integration;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/api")
public class RestApplication extends Application {
    // This class can be empty.
    // Its purpose is to provide the @ApplicationPath annotation,
    // which activates and configures JAX-RS.

}