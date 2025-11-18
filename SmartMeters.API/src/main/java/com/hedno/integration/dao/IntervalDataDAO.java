package com.hedno.integration.dao;

import com.hedno.integration.processor.IntervalData;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * DAO for batch-inserting parsed interval data into
 * the new SMC_LOAD_PROFILE_INTERVALS table.
 */
public class IntervalDataDAO {

    private static final Logger logger = LoggerFactory.getLogger(IntervalDataDAO.class);

    // SQL for the new intervals table
    private static final String BATCH_INSERT_SQL =
        "INSERT INTO SMC_LOAD_PROFILE_INTERVALS (ITEM_ID, INTERVAL_START_TIME, " +
        "INTERVAL_END_TIME, INTERVAL_VALUE, UNIT_CODE, STATUS) " +
        "VALUES (?, ?, ?, ?, ?, ?)";

    private DataSource dataSource;

    public IntervalDataDAO() {
        initializeDataSource();
    }

    /**
     * Initializes the data source, falling back to HikariCP if JNDI fails.
     */
    private void initializeDataSource() {
        try {
            InitialContext ctx = new InitialContext();
            this.dataSource = (DataSource) ctx.lookup("java:comp/env/jdbc/LoadProfileDB");
            logger.info("IntervalDataDAO: Successfully obtained DataSource from JNDI");
        } catch (NamingException e) {
            logger.warn("IntervalDataDAO: JNDI DataSource not found, creating HikariCP pool", e);
            createHikariDataSource();
        }
    }

    private void createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(System.getProperty("db.url", "jdbc:oracle:thin:@localhost:1521:XE"));
        config.setUsername(System.getProperty("db.username", "LOAD_PROFILE"));
        config.setPassword(System.getProperty("db.password", "password"));
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setMaximumPoolSize(20);
        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Inserts a list of intervals as a single high-performance batch.
     *
     * @param orderItemId The parent ITEM_ID from SMC_ORDER_ITEMS.
     * @param intervals   The list of 96 intervals to insert.
     * @return true if successful, false otherwise.
     */
    public boolean batchInsertIntervals(long orderItemId, List<IntervalData> intervals) {
        if (intervals == null || intervals.isEmpty()) {
            logger.warn("No intervals to insert for item ID {}", orderItemId);
            return true; // Not an error
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(BATCH_INSERT_SQL)) {

            conn.setAutoCommit(false); // Start transaction

            for (IntervalData interval : intervals) {
                ps.setLong(1, orderItemId);
                ps.setTimestamp(2, Timestamp.valueOf(interval.getStartDateTime()));
                ps.setTimestamp(3, Timestamp.valueOf(interval.getEndDateTime()));
                ps.setBigDecimal(4, interval.getValue());
                ps.setString(5, interval.getUnitCode());
                ps.setString(6, interval.getStatus());
                ps.addBatch();
            }

            int[] results = ps.executeBatch();
            conn.commit(); // Commit transaction

            logger.info("Successfully inserted {} intervals for item ID {}", results.length, orderItemId);
            return true;

        } catch (SQLException e) {
            logger.error("Error during batch insert for item ID {}", orderItemId, e);
            // Connection auto-rolls-back on close if not committed
            return false;
        }
    }
    
    public void close() {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }
}