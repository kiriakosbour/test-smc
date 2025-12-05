package com.hedno.integration.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Oracle Database Connection Factory
 * 
 * Supports two connection modes:
 * 1. JNDI lookup (WebLogic/Production) - uses datasource.jndi.name
 * 2. Direct JDBC (Development) - uses jdbc.url, jdbc.username, jdbc.password
 * 
 * Configuration is read from application.properties (filtered by Maven profile)
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class ConnectOracleDAO {

    private static final Logger log = LoggerFactory.getLogger(ConnectOracleDAO.class);
    
    private final Properties properties;
    private DataSource cachedDataSource;
    private boolean jndiLookupAttempted = false;
    
    public ConnectOracleDAO(Properties properties) {
        this.properties = properties;
    }
    
    /**
     * Get a database connection.
     * First attempts JNDI lookup, falls back to direct JDBC if JNDI fails.
     * 
     * @return Connection to Oracle database
     * @throws Exception if connection cannot be established
     */
    public Connection getConnection() throws Exception {
        // Try JNDI first (WebLogic environment)
        Connection conn = getJndiConnection();
        if (conn != null) {
            return conn;
        }
        
        // Fallback to direct JDBC (development environment)
        return getDirectConnection();
    }
    
    /**
     * Attempt to get connection via JNDI lookup
     */
    private Connection getJndiConnection() {
        String jndiName = properties.getProperty("datasource.jndi.name");
        
        if (jndiName == null || jndiName.trim().isEmpty() || jndiName.contains("${")) {
            log.debug("JNDI datasource name not configured, skipping JNDI lookup");
            return null;
        }
        
        try {
            // Use cached datasource if available
            if (cachedDataSource != null) {
                Connection conn = cachedDataSource.getConnection();
                log.debug("Got connection from cached JNDI DataSource: {}", jndiName);
                return conn;
            }
            
            // Only attempt lookup once to avoid repeated failures
            if (jndiLookupAttempted) {
                return null;
            }
            jndiLookupAttempted = true;
            
            log.info("Attempting JNDI lookup for: {}", jndiName);
            
            Context ctx = new InitialContext();
            cachedDataSource = (DataSource) ctx.lookup(jndiName);
            
            Connection conn = cachedDataSource.getConnection();
            log.info("Successfully connected via JNDI: {}", jndiName);
            return conn;
            
        } catch (Exception e) {
            log.warn("JNDI lookup failed for '{}': {} - will use direct JDBC", 
                jndiName, e.getMessage());
            cachedDataSource = null;
            return null;
        }
    }
    
    /**
     * Get connection via direct JDBC (fallback for development)
     */
    private Connection getDirectConnection() throws Exception {
        String url = properties.getProperty("jdbc.url");
        String username = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");
        String driver = properties.getProperty("jdbc.driver", "oracle.jdbc.OracleDriver");
        
        if (url == null || url.trim().isEmpty() || url.contains("${")) {
            throw new IllegalStateException(
                "Database connection not configured. " +
                "Set either datasource.jndi.name (JNDI) or jdbc.url (direct JDBC) in application.properties");
        }
        
        log.debug("Connecting via direct JDBC to: {}", url);
        
        // Load driver
        Class.forName(driver);
        
        // Get connection
        Connection conn = DriverManager.getConnection(url, username, password);
        log.debug("Successfully connected via direct JDBC");
        
        return conn;
    }
    
    /**
     * Get current environment from properties
     */
    public String getEnvironment() {
        return properties.getProperty("app.environment", "unknown");
    }
    
    /**
     * Check if using JNDI connection
     */
    public boolean isUsingJndi() {
        return cachedDataSource != null;
    }
}
