package com.hedno.integration.dao;

import com.hedno.integration.ConfigService;
import com.hedno.integration.entity.OrderItem;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import java.io.StringReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for SMC_ORDER_PACKAGES and SMC_ORDER_ITEMS tables.
 * Handles the creation of order items and the batching of order packages.
 *
 * @author HEDNO Integration Team
 * @version 1.1 - Simplified CLOB handling
 */
public class OrderPackageDAO {

    private static final Logger logger = LoggerFactory.getLogger(OrderPackageDAO.class);

    // SQL for finding packages ready for processing
    private static final String SELECT_READY_PACKAGES_SQL = "SELECT PACKAGE_ID FROM (" +
            "  SELECT " +
            "    p.PACKAGE_ID, " +
            "    p.CREATED_TIMESTAMP, " +
            "    COUNT(i.ITEM_ID) as ITEM_COUNT " +
            "  FROM SMC_ORDER_PACKAGES p " +
            "  JOIN SMC_ORDER_ITEMS i ON p.PACKAGE_ID = i.PACKAGE_ID " +
            "  WHERE p.STATUS = 'OPEN' " +
            "  GROUP BY p.PACKAGE_ID, p.CREATED_TIMESTAMP " +
            ") " +
            "WHERE (CREATED_TIMESTAMP < (SYSTIMESTAMP - (? / 1440.0))) " + // Param 1: maxAgeMinutes
            "   OR (ITEM_COUNT >= ?)"; // Param 2: maxSize

    // SQL for getting items for a package
    private static final String SELECT_ITEMS_BY_PACKAGE_SQL = "SELECT ITEM_ID, PACKAGE_ID, PROFIL_BLOC_ID, DATA_TYPE, OBIS_CODE, POD_ID, STATUS, CREATED_TIMESTAMP, RAW_XML "
            +
            "FROM SMC_ORDER_ITEMS " +
            "WHERE PACKAGE_ID = ?";
    // SQL for updating package status
    private static final String UPDATE_PACKAGE_STATUS_SQL = "UPDATE SMC_ORDER_PACKAGES SET STATUS = ? WHERE PACKAGE_ID = ?";

    // SQL for marking items as processed
    private static final String UPDATE_ITEM_STATUS_SQL = "UPDATE SMC_ORDER_ITEMS SET STATUS = ? WHERE PACKAGE_ID = ?";

    private DataSource dataSource;

    public OrderPackageDAO() {
        initializeDataSource();
    }

    /**
     * Constructor with explicit DataSource (for testing).
     */
    public OrderPackageDAO(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private void initializeDataSource() {
        // REFACTORED: Use property for JNDI name
        String jndiName = ConfigService.get("jndi.datasource.name", "java:comp/env/jdbc/LoadProfileDB");
        try {
            InitialContext ctx = new InitialContext();
            this.dataSource = (DataSource) ctx.lookup(jndiName);
            logger.info("OrderPackageDAO: Successfully obtained DataSource from JNDI: {}", jndiName);
        } catch (NamingException e) {
            logger.warn("OrderPackageDAO: JNDI DataSource '{}' not found, creating HikariCP pool: {}", jndiName,
                    e.getMessage());
            createHikariDataSource();
        }
    }

    private void createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(ConfigService.get("db.url", "jdbc:oracle:thin:@localhost:1521:XE"));
        config.setUsername(ConfigService.get("db.username", "LOAD_PROFILE"));
        config.setPassword(ConfigService.get("db.password", "password"));
        config.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        config.setMaximumPoolSize(Integer.parseInt(ConfigService.get("db.pool.size.max", "20")));
        config.setMinimumIdle(Integer.parseInt(ConfigService.get("db.pool.size.min", "5")));
        config.setConnectionTimeout(Long.parseLong(ConfigService.get("db.connection.timeout", "30000")));
        config.setIdleTimeout(Long.parseLong(ConfigService.get("db.pool.idle.timeout", "600000")));
        config.setMaxLifetime(Long.parseLong(ConfigService.get("db.pool.max.lifetime", "1800000")));

        this.dataSource = new HikariDataSource(config);
        logger.info("OrderPackageDAO: HikariCP DataSource initialized successfully");
    }

    // Stored Procedure call
    private static final String CALL_ADD_ORDER_ITEM_SP = "{CALL SP_SMC_ADD_ORDER_ITEM(?, ?, ?, ?, ?, ?, ?)}";

    /**
     * Creates a new order item and assigns it to an open package
     * using the SP_SMC_ADD_ORDER_ITEM stored procedure.
     *
     * @param channelId    The logical grouping ID (e.g., Meter ID)
     * @param profilBlocId The ID from PROFIL_BLOC
     * @param dataType     The data type (e.g., "Energie / Consumption")
     * @param obisCode     The OBIS code for the item
     * @param podId        The POD ID for the item
     * @param rawXml       The complete raw XML payload
     * @return The ID of the package the item was added to.
     * @throws SQLException if the database call fails
     */
    public long createOrderItem(String channelId, String profilBlocId, String dataType, String obisCode, String podId,
            String rawXml) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                CallableStatement cs = conn.prepareCall(CALL_ADD_ORDER_ITEM_SP)) {

            conn.setAutoCommit(false); // Manage transaction

            cs.setString(1, channelId);
            cs.setString(2, profilBlocId);
            cs.setString(3, dataType);
            cs.setString(4, obisCode);
            cs.setString(5, podId);
            if (rawXml != null && !rawXml.isEmpty()) {
                // Use setCharacterStream - bypasses WebLogic's SerialClob issue
                // This streams the data directly without creating a CLOB object
                cs.setCharacterStream(6, new StringReader(rawXml), rawXml.length());
            } else {
                cs.setNull(6, Types.CLOB);
            }
            cs.registerOutParameter(7, Types.NUMERIC); // p_package_id OUT

            cs.execute();

            long packageId = cs.getLong(7);

            conn.commit(); // Commit transaction
            logger.debug("Successfully added item for profil_bloc {} to package {}", profilBlocId, packageId);
            return packageId;

        } catch (SQLException e) {
            logger.error("Error calling SP_SMC_ADD_ORDER_ITEM for profil_bloc_id: {}", profilBlocId, e);
            throw e; // Re-throw to inform the caller
        }
    }

    /**
     * Finds all package IDs that are ready to be processed based on age or size.
     *
     * @param maxAgeMinutes Max age in minutes
     * @param maxSize       Max number of items
     * @return A list of package IDs
     */
    public List<Long> findReadyPackages(int maxAgeMinutes, int maxSize) {
        List<Long> packageIds = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(SELECT_READY_PACKAGES_SQL)) {

            ps.setInt(1, maxAgeMinutes);
            ps.setInt(2, maxSize);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    packageIds.add(rs.getLong("PACKAGE_ID"));
                }
            }
            logger.debug("Found {} packages ready for processing.", packageIds.size());
        } catch (SQLException e) {
            logger.error("Error finding ready packages", e);
        }
        return packageIds;
    }

    /**
     * Retrieves all OrderItems for a specific package.
     *
     * @param packageId The ID of the package
     * @return A list of OrderItem entities
     */
    public List<OrderItem> getItemsForPackage(long packageId) {
        List<OrderItem> items = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(SELECT_ITEMS_BY_PACKAGE_SQL)) {

            ps.setLong(1, packageId);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(mapResultSetToOrderItem(rs));
                }
            }
            logger.debug("Retrieved {} items for package {}", items.size(), packageId);
        } catch (SQLException e) {
            logger.error("Error getting items for package: {}", packageId, e);
        }
        return items;
    }

    /**
     * Updates the status of a specific package.
     *
     * @param packageId The package to update
     * @param status    The new status
     * @return true if successful
     */
    public boolean updatePackageStatus(long packageId, com.hedno.integration.entity.OrderPackage.PackageStatus status) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(UPDATE_PACKAGE_STATUS_SQL)) {

            ps.setString(1, status.getValue());
            ps.setLong(2, packageId);

            int rowsAffected = ps.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated package {} status to {}", packageId, status.getValue());
                return true;
            }
            return false;
        } catch (SQLException e) {
            logger.error("Error updating package status for package: {}", packageId, e);
            return false;
        }
    }

    /**
     * Updates the status of all items within a package.
     *
     * @param packageId The package containing the items
     * @param status    The new status for the items
     * @return true if successful
     */
    public boolean updateItemStatusForPackage(long packageId, OrderItem.ItemStatus status) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(UPDATE_ITEM_STATUS_SQL)) {

            ps.setString(1, status.getValue());
            ps.setLong(2, packageId);

            int rowsAffected = ps.executeUpdate();
            logger.info("Updated {} items in package {} to status {}", rowsAffected, packageId, status.getValue());
            return true;
        } catch (SQLException e) {
            logger.error("Error updating item statuses for package: {}", packageId, e);
            return false;
        }
    }

    /**
     * Helper method to map a ResultSet row to an OrderItem entity.
     */
    private OrderItem mapResultSetToOrderItem(ResultSet rs) throws SQLException {
        OrderItem item = new OrderItem();
        item.setItemId(rs.getLong("ITEM_ID"));
        item.setPackageId(rs.getLong("PACKAGE_ID"));
        item.setProfilBlocId(rs.getString("PROFIL_BLOC_ID"));
        item.setDataType(rs.getString("DATA_TYPE"));
        item.setObisCode(rs.getString("OBIS_CODE"));
        item.setPodId(rs.getString("POD_ID"));
        item.setStatus(OrderItem.ItemStatus.fromValue(rs.getString("STATUS")));
        item.setCreatedTimestamp(rs.getTimestamp("CREATED_TIMESTAMP"));
        Clob clob = rs.getClob("RAW_XML");
        if (clob != null) {
            item.setRawXml(clob.getSubString(1, (int) clob.length()));
        }
        return item;
    }

    private static final String CHECK_CHANNEL_RELEVANT_SQL = "SELECT 1 FROM SMC_PUSH_RELEVANT_CHANNELS WHERE CHANNEL_ID = ? AND IS_RELEVANT = 1";

    /**
     * Checks if a given channel ID is marked as "push relevant" in the
     * configuration table.
     *
     * @param channelId The channel ID to check
     * @return true if the channel is relevant for pushing, false otherwise
     */
    public boolean isChannelPushRelevant(String channelId) {
        if (channelId == null) {
            return false;
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(CHECK_CHANNEL_RELEVANT_SQL)) {

            ps.setString(1, channelId);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next(); // true if a record was found
            }
        } catch (SQLException e) {
            logger.error("Error checking push relevance for channel: {}", channelId, e);
            return false; // Fail-safe: assume not relevant if DB error
        }
    }

    // SQL for marking a single item as processed
    private static final String UPDATE_SINGLE_ITEM_STATUS_SQL = "UPDATE SMC_ORDER_ITEMS SET STATUS = ? WHERE ITEM_ID = ?";

    /**
     * Updates the status of a single item.
     * Used by the synchronous endpoint after intervals are saved.
     * 
     * @param itemId The item to update
     * @param status The new status
     * @return true if successful
     */
    public boolean updateItemStatus(long itemId, OrderItem.ItemStatus status) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(UPDATE_SINGLE_ITEM_STATUS_SQL)) {

            ps.setString(1, status.getValue());
            ps.setLong(2, itemId);

            int rowsAffected = ps.executeUpdate();
            if (rowsAffected > 0) {
                logger.info("Updated item {} to status {}", itemId, status.getValue());
                return true;
            }
            return false;
        } catch (SQLException e) {
            logger.error("Error updating item status for item: {}", itemId, e);
            return false;
        }
    }

    /**
     * Closes the data source if it's a HikariDataSource.
     */
    public void close() {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
            logger.info("OrderPackageDAO: DataSource closed successfully");
        }
    }
}
