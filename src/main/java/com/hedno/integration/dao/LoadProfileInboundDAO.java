package com.hedno.integration.dao;

import com.hedno.integration.entity.LoadProfileInbound;
import com.hedno.integration.entity.LoadProfileInbound.ProcessingStatus;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.*;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for LOAD_PROFILE_INBOUND table operations.
 * Implements database operations with proper resource management and error handling.
 * * @author HEDNO Integration Team
 * @version 1.1 (Fixed)
 */
public class LoadProfileInboundDAO {
    
    private static final Logger logger = LoggerFactory.getLogger(LoadProfileInboundDAO.class);
    
    // FIXED: Aligned with v2.0 Entity (added originalMessageId, removed attemptCount from insert)
    private static final String INSERT_SQL = 
        "INSERT INTO SMC_LOAD_PROFILE_INBOUND (MESSAGE_UUID, RAW_PAYLOAD, STATUS, RECEIVED_TIMESTAMP, " +
        "LAST_ERROR_MESSAGE, ORIGINAL_MESSAGE_ID) VALUES (?, ?, ?, ?, ?, ?)";
    
    // FIXED: Aligned with v2.0 Entity and Processor logic
    // The processor now updates all fields, so this query must be comprehensive.
    private static final String UPDATE_STATUS_SQL = 
        "UPDATE SMC_LOAD_PROFILE_INBOUND SET STATUS = ?, " +
        "PROCESSING_START_TIME = ?, PROCESSING_END_TIME = ?, " +
        "LAST_HTTP_STATUS_CODE = ?, LAST_RESPONSE_MESSAGE = ?, " +
        "LAST_ERROR_MESSAGE = ?, ORIGINAL_MESSAGE_ID = ?, " +
        "LAST_ATTEMPT_TIMESTAMP = ?, ATTEMPT_COUNT = ?, " + 
        "MANUAL_RETRY_COUNT = ? " +
        "WHERE MESSAGE_UUID = ?";
    
    // FIXED: Aligned with v2.0 Entity (added all new fields)
    private static final String SELECT_BY_UUID_SQL = 
        "SELECT MESSAGE_UUID, RAW_PAYLOAD, STATUS, RECEIVED_TIMESTAMP, " +
        "PROCESSING_START_TIME, PROCESSING_END_TIME, LAST_MANUAL_RETRY_TIME, " +
        "LAST_HTTP_STATUS_CODE, LAST_RESPONSE_MESSAGE, LAST_ERROR_MESSAGE, " +
        "ORIGINAL_MESSAGE_ID, MANUAL_RETRY_COUNT, PROCESSED_BY, NOTES, " +
        "LAST_ATTEMPT_TIMESTAMP, ATTEMPT_COUNT " +
        "FROM SMC_LOAD_PROFILE_INBOUND WHERE MESSAGE_UUID = ?";

    // FIXED: Aligned with v3.0 Processor logic (fetches PENDING, no retry logic)
    private static final String SELECT_FOR_PROCESSING_SQL = 
        "SELECT MESSAGE_UUID, RAW_PAYLOAD, STATUS, RECEIVED_TIMESTAMP, " +
        "PROCESSING_START_TIME, PROCESSING_END_TIME, LAST_MANUAL_RETRY_TIME, " +
        "LAST_HTTP_STATUS_CODE, LAST_RESPONSE_MESSAGE, LAST_ERROR_MESSAGE, " +
        "ORIGINAL_MESSAGE_ID, MANUAL_RETRY_COUNT, PROCESSED_BY, NOTES, " +
        "LAST_ATTEMPT_TIMESTAMP, ATTEMPT_COUNT " +
        "FROM SMC_LOAD_PROFILE_INBOUND " +
        "WHERE STATUS = 'PENDING' " + // Changed from ('RECEIVED', 'FAILED_RETRY')
        "ORDER BY RECEIVED_TIMESTAMP " +
        "FETCH FIRST ? ROWS ONLY"; // Only one parameter now
    
    // FIXED: Aligned with v2.0 Entity (added all new fields)
    private static final String SELECT_BY_STATUS_SQL = 
        "SELECT MESSAGE_UUID, RAW_PAYLOAD, STATUS, RECEIVED_TIMESTAMP, " +
        "PROCESSING_START_TIME, PROCESSING_END_TIME, LAST_MANUAL_RETRY_TIME, " +
        "LAST_HTTP_STATUS_CODE, LAST_RESPONSE_MESSAGE, LAST_ERROR_MESSAGE, " +
        "ORIGINAL_MESSAGE_ID, MANUAL_RETRY_COUNT, PROCESSED_BY, NOTES, " +
        "LAST_ATTEMPT_TIMESTAMP, ATTEMPT_COUNT " +
        "FROM SMC_LOAD_PROFILE_INBOUND " +
        "WHERE STATUS = ? ORDER BY RECEIVED_TIMESTAMP";
    
    private static final String CHECK_EXISTS_SQL = 
        "SELECT 1 FROM SMC_LOAD_PROFILE_INBOUND WHERE MESSAGE_UUID = ?";
    
    // NOTE: This DLQ logic seems obsolete based on v3.0 processor
    private static final String UPDATE_TO_DLQ_SQL = 
        "UPDATE SMC_LOAD_PROFILE_INBOUND SET STATUS = 'FAILED_DLQ', LAST_ATTEMPT_TIMESTAMP = ?, " +
        "LAST_ERROR_MESSAGE = ? WHERE MESSAGE_UUID = ? AND ATTEMPT_COUNT >= ?";
    
    private DataSource dataSource;
    
    /**
     * Constructor that initializes the data source.
     * First attempts JNDI lookup for WebLogic, falls back to HikariCP if not found.
     */
    public LoadProfileInboundDAO() {
        initializeDataSource();
    }
    
    /**
     * Constructor with explicit DataSource (useful for testing)
     */
    public LoadProfileInboundDAO(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    /**
     * Initialize the data source - try JNDI first, then fallback to HikariCP
     */
    private void initializeDataSource() {
        // First try JNDI lookup for WebLogic datasource
        try {
            InitialContext ctx = new InitialContext();
                      this.dataSource = (DataSource) ctx.lookup("java:comp/env/jdbc/LoadProfileDB");

            logger.info("Successfully obtained DataSource from JNDI");
        } catch (NamingException e) {
            logger.warn("JNDI DataSource not found, creating HikariCP pool: {}", e.getMessage());
            createHikariDataSource();
        }
    }
    
    /**
     * Create HikariCP data source as fallback
     */
    private void createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        
        // Read from system properties or use defaults
        config.setJdbcUrl(System.getProperty("db.url", 
            "jdbc:oracle:thin:@localhost:1521:XE"));
        config.setUsername(System.getProperty("db.username", "LOAD_PROFILE"));
        config.setPassword(System.getProperty("db.password", "password"));
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        
        // Connection pool settings
        config.setMaximumPoolSize(20);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000); // 30 seconds
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes
        config.setLeakDetectionThreshold(60000); // 1 minute
        
        // Performance optimizations
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        
        this.dataSource = new HikariDataSource(config);
        logger.info("HikariCP DataSource initialized successfully");
    }
    
    /**
     * Insert a new load profile inbound record
     * @param loadProfile The load profile entity to insert
     * @return true if insert successful, false otherwise
     */
    public boolean insert(LoadProfileInbound loadProfile) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            
            ps.setString(1, loadProfile.getMessageUuid());
            ps.setClob(2, new StringReader(loadProfile.getRawPayload()));
            ps.setString(3, loadProfile.getStatus().getValue());
            ps.setTimestamp(4, loadProfile.getReceivedTimestamp());
            ps.setString(5, loadProfile.getLastErrorMessage());
            ps.setString(6, loadProfile.getOriginalMessageId()); // <-- ADDED
            
            int rowsAffected = ps.executeUpdate();
            
            if (rowsAffected > 0) {
                logger.info("Successfully inserted load profile with UUID: {}", 
                    loadProfile.getMessageUuid());
                return true;
            }
            return false;
            
        } catch (SQLException e) {
            logger.error("Error inserting load profile with UUID: {}", 
                loadProfile.getMessageUuid(), e);
            return false;
        }
    }
    
    /**
     * Check if a message with given UUID already exists
     * @param messageUuid The message UUID to check
     * @return true if exists, false otherwise
     */
    public boolean exists(String messageUuid) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CHECK_EXISTS_SQL)) {
            
            ps.setString(1, messageUuid);
            
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
            
        } catch (SQLException e) {
            logger.error("Error checking existence for UUID: {}", messageUuid, e);
            return false;
        }
    }
    
    /**
     * Update the status of a load profile record
     * @param loadProfile The load profile with updated status
     * @return true if update successful, false otherwise
     */
    public boolean updateStatus(LoadProfileInbound loadProfile) {
        // FIXED: This method now updates all fields based on processor's logic
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATUS_SQL)) {
            
            ps.setString(1, loadProfile.getStatus().getValue());
            ps.setTimestamp(2, loadProfile.getProcessingStartTime());
            ps.setTimestamp(3, loadProfile.getProcessingEndTime());
            ps.setInt(4, loadProfile.getLastHttpStatusCode());
            ps.setString(5, loadProfile.getLastResponseMessage());
            ps.setString(6, loadProfile.getLastErrorMessage());
            ps.setString(7, loadProfile.getOriginalMessageId());
            ps.setTimestamp(8, loadProfile.getLastAttemptTimestamp());
            ps.setInt(9, loadProfile.getAttemptCount());
            ps.setInt(10, loadProfile.getManualRetryCount());
            ps.setString(11, loadProfile.getMessageUuid());
            
            int rowsAffected = ps.executeUpdate();
            
            if (rowsAffected > 0) {
                logger.debug("Updated status to {} for UUID: {}", 
                    loadProfile.getStatus(), loadProfile.getMessageUuid());
                return true;
            }
            return false;
            
        } catch (SQLException e) {
            logger.error("Error updating status for UUID: {}", 
                loadProfile.getMessageUuid(), e);
            return false;
        }
    }
    
    /**
     * Find a load profile by UUID
     * @param messageUuid The message UUID
     * @return LoadProfileInbound or null if not found
     */
    public LoadProfileInbound findByUuid(String messageUuid) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_UUID_SQL)) {
            
            ps.setString(1, messageUuid);
            
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToEntity(rs);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Error finding load profile by UUID: {}", messageUuid, e);
        }
        return null;
    }
    
    /**
     * Get messages ready for processing (PENDING status)
     * @param batchSize Number of records to fetch
     * @return List of LoadProfileInbound ready for processing
     */
    public List<LoadProfileInbound> getMessagesForProcessing(int batchSize) {
        List<LoadProfileInbound> messages = new ArrayList<>();
        
        // FIXED: Query logic aligned with v3.0 Processor
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_FOR_PROCESSING_SQL)) {
            
            // FIXED: Only one parameter now
            ps.setInt(1, batchSize);
            
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    messages.add(mapResultSetToEntity(rs));
                }
            }
            
            if (messages.size() > 0) {
                 logger.info("Retrieved {} messages for processing", messages.size());
            }
            
        } catch (SQLException e) {
            logger.error("Error retrieving messages for processing", e);
        }
        
        return messages;
    }
    
    /**
     * Find all messages by status
     * @param status The status to filter by
     * @return List of LoadProfileInbound with given status
     */
    public List<LoadProfileInbound> findByStatus(ProcessingStatus status) {
        List<LoadProfileInbound> messages = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_STATUS_SQL)) {
            
            ps.setString(1, status.getValue());
            
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    messages.add(mapResultSetToEntity(rs));
                }
            }
            
            logger.debug("Found {} messages with status {}", messages.size(), status);
            
        } catch (SQLException e) {
            logger.error("Error finding messages by status: {}", status, e);
        }
        
        return messages;
    }
    
    /**
     * Move message to Dead Letter Queue after max retries exceeded
     * (NOTE: This logic appears obsolete with v3.0 processor)
     * @param messageUuid The message UUID
     * @param maxRetries Max retries threshold
     * @param errorMessage Final error message
     * @return true if moved to DLQ, false otherwise
     */
    public boolean moveToDeadLetterQueue(String messageUuid, int maxRetries, String errorMessage) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_TO_DLQ_SQL)) {
            
            ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            ps.setString(2, errorMessage);
            ps.setString(3, messageUuid);
            ps.setInt(4, maxRetries);
            
            int rowsAffected = ps.executeUpdate();
            
            if (rowsAffected > 0) {
                logger.warn("Message {} moved to Dead Letter Queue", messageUuid);
                return true;
            }
            return false;
            
        } catch (SQLException e) {
            logger.error("Error moving message {} to DLQ", messageUuid, e);
            return false;
        }
    }
    
    /**
     * Map ResultSet to LoadProfileInbound entity
     */
    private LoadProfileInbound mapResultSetToEntity(ResultSet rs) throws SQLException {
        LoadProfileInbound entity = new LoadProfileInbound();
        
        entity.setMessageUuid(rs.getString("MESSAGE_UUID"));
        
        // Handle CLOB
        Clob clob = rs.getClob("RAW_PAYLOAD");
        if (clob != null) {
            entity.setRawPayload(clob.getSubString(1, (int) clob.length()));
        }
        
        // FIXED: Use fromValue to handle new/old status mapping
        entity.setStatus(ProcessingStatus.fromValue(rs.getString("STATUS")));
        entity.setReceivedTimestamp(rs.getTimestamp("RECEIVED_TIMESTAMP"));
        
        // FIXED: Map all new fields from v2.0 Entity
        entity.setProcessingStartTime(rs.getTimestamp("PROCESSING_START_TIME"));
        entity.setProcessingEndTime(rs.getTimestamp("PROCESSING_END_TIME"));
        entity.setLastManualRetryTime(rs.getTimestamp("LAST_MANUAL_RETRY_TIME"));
        entity.setLastHttpStatusCode(rs.getInt("LAST_HTTP_STATUS_CODE"));
        entity.setLastResponseMessage(rs.getString("LAST_RESPONSE_MESSAGE"));
        entity.setLastErrorMessage(rs.getString("LAST_ERROR_MESSAGE"));
        entity.setOriginalMessageId(rs.getString("ORIGINAL_MESSAGE_ID"));
        entity.setManualRetryCount(rs.getInt("MANUAL_RETRY_COUNT"));
        entity.setProcessedBy(rs.getString("PROCESSED_BY"));
        entity.setNotes(rs.getString("NOTES"));
        
        // Map old fields
        entity.setLastAttemptTimestamp(rs.getTimestamp("LAST_ATTEMPT_TIMESTAMP"));
        entity.setAttemptCount(rs.getInt("ATTEMPT_COUNT"));
        
        return entity;
    }
    
    /**
     * Close the data source (for shutdown)
     */
    public void close() {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
            logger.info("DataSource closed successfully");
        }
    }
}