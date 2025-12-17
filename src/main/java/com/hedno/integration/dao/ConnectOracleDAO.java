package com.hedno.integration.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hedno.integration.ConfigService;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Oracle Database Connection DAO with dual-mode support:
 * 1. JNDI DataSource lookup (WebLogic/Production)
 * 2. Direct JDBC connection (Development fallback)
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class ConnectOracleDAO {

    private static final Logger logger = LoggerFactory.getLogger(ConnectOracleDAO.class);

    private DataSource dataSource;
    private boolean jndiAttempted = false;
    private boolean jndiAvailable = false;

    // Direct JDBC configuration (fallback)
    private final String driver;
    private final String connectString;
    private final String username;
    private final String password;
    private final String jndiName;

    /**
     * Constructor with Properties configuration
     * 
     * @param props Properties containing database configuration
     */
    public ConnectOracleDAO() {
        this.jndiName = ConfigService.get("jndi.datasource.name", "jdbc/artemis_smc");
        this.driver = ConfigService.get("db.driver", "oracle.jdbc.driver.OracleDriver");
        this.connectString = ConfigService.get("db.url", "");
        this.username = ConfigService.get("db.username", "");
        this.password = ConfigService.get("db.password", "");

        logger.debug("ConnectOracleDAO initialized - JNDI: {}, JDBC URL: {}", 
            jndiName, connectString);
    }

    /**
     * Initialize DataSource via JNDI lookup
     * Only attempts once to avoid repeated failed lookups
     */
    private synchronized void initializeDataSource() {
        if (jndiAttempted) {
            return;
        }
        jndiAttempted = true;

        try {
            InitialContext ctx = new InitialContext();
            dataSource = (DataSource) ctx.lookup(jndiName);
            jndiAvailable = true;
            logger.info("Successfully obtained DataSource from JNDI: {}", jndiName);
        } catch (NamingException e) {
            logger.warn("JNDI DataSource '{}' not available, will use direct JDBC: {}", 
                jndiName, e.getMessage());
            jndiAvailable = false;
        }
    }

    /**
     * Get a database connection
     * Tries JNDI first, falls back to direct JDBC
     * 
     * @return Database connection
     * @throws SQLException if connection cannot be established
     */
    public Connection getConnection() throws SQLException {
        // Try JNDI DataSource first
        if (!jndiAttempted) {
            initializeDataSource();
        }

        if (jndiAvailable && dataSource != null) {
            try {
                Connection conn = dataSource.getConnection();
                logger.debug("Connection obtained from JNDI DataSource");
                return conn;
            } catch (SQLException e) {
                logger.error("Failed to get connection from JNDI DataSource", e);
                throw e;
            }
        }

        // Fallback to direct JDBC
        return getDirectConnection();
    }

    /**
     * Get connection via direct JDBC (fallback for development)
     * 
     * @return Database connection
     * @throws SQLException if connection cannot be established
     */
    private Connection getDirectConnection() throws SQLException {
        if (connectString == null || connectString.isEmpty()) {
            throw new SQLException("No JNDI DataSource available and JDBC URL is not configured");
        }

        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(connectString, username, password);
            logger.debug("Connection obtained via direct JDBC: {}", connectString);
            return conn;
        } catch (ClassNotFoundException e) {
            logger.error("Oracle JDBC driver not found: {}", driver, e);
            throw new SQLException("JDBC driver not found: " + driver, e);
        } catch (SQLException e) {
            logger.error("Failed to establish direct JDBC connection to: {}", connectString, e);
            throw e;
        }
    }

    /**
     * Check if JNDI DataSource is available
     */
    public boolean isJndiAvailable() {
        if (!jndiAttempted) {
            initializeDataSource();
        }
        return jndiAvailable;
    }

    /**
     * Get connection mode description for logging
     */
    public String getConnectionMode() {
        if (!jndiAttempted) {
            initializeDataSource();
        }
        return jndiAvailable ? "JNDI:" + jndiName : "JDBC:" + connectString;
    }
}
