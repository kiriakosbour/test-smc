package com.hedno.integration.service;

import com.hedno.integration.dao.ConnectOracleDAO;
import com.hedno.integration.processor.IntervalData;
import com.hedno.integration.processor.LoadProfileData;
import com.hedno.integration.processor.LoadProfileDataExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * MDM Import Service v3.0
 * 
 * Processes incoming MDM data and stores it for Artemis consumption:
 * - SMC_MDM_SCCURVES_HD: Master/header table
 * - SMC_MDM_SCCURVES: Curve data with HD_LOG_ID FK
 * - Multi-source support: ZFA, ITRON
 * - Multi-type support: MEASURE, ALARM, EVENT
 * - SECTION_UUID tracking per channel
 * - UTC to Greek timezone conversion
 * - Horizontal pivot (Q1-Q96 columns)
 * 
 * Note: No SAP integration - Artemis reads directly from database.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class MdmImportService {

    private static final Logger log = LoggerFactory.getLogger(MdmImportService.class);

    // Timezone for Greek local time conversion
    private static final ZoneId GREEK_ZONE = ZoneId.of("Europe/Athens");
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    // Source system constants
    public static final String SOURCE_SYSTEM_ZFA = "ZFA";
    public static final String SOURCE_SYSTEM_ITRON = "ITRON";

    // Source type constants
    public static final String SOURCE_TYPE_MEASURE = "MEASURE";
    public static final String SOURCE_TYPE_ALARM = "ALARM";
    public static final String SOURCE_TYPE_EVENT = "EVENT";

    // Max intervals (96 normal + 4 for DST)
    private static final int MAX_INTERVALS = 100;

    private final LoadProfileDataExtractor extractor;


    // SQL Statements
    private static final String INSERT_HD_SQL =
        "INSERT INTO SMC_MDM_SCCURVES_HD (" +
        "SOURCE_SYSTEM, SOURCE_TYPE, FILE_ID, FILE_NAME, MESSAGE_UUID, " +
        "WSDL_OPERATION, ENDPOINT, SENDER_ID, RECIPIENT_ID, SOURCE_CREATION_DT, " +
        "STATUS, RAW_XML) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)";

    private static final String UPDATE_HD_STATUS_SQL =
        "UPDATE SMC_MDM_SCCURVES_HD SET STATUS = ?, STATUS_MSG = ?, " +
        "RECORDS_PROCESSED = ?, DT_UPDATE = SYSTIMESTAMP WHERE LOG_ID = ?";

    private static final String SELECT_HD_SQL =
        "SELECT LOG_ID, SOURCE_SYSTEM, SOURCE_TYPE, MESSAGE_UUID, STATUS, STATUS_MSG, " +
        "RECORDS_PROCESSED, RECEIVED_AT FROM SMC_MDM_SCCURVES_HD WHERE LOG_ID = ?";

    private static final String SELECT_HD_BY_UUID_SQL =
        "SELECT STATUS FROM SMC_MDM_SCCURVES_HD WHERE MESSAGE_UUID = ?";

    private static final String SELECT_CURVES_SUMMARY_SQL =
        "SELECT POD_ID, SUPPLY_NUM, DATA_CLASS, DATE_READ, SECTION_UUID, " +
        "SOURCE_CREATION_DT, DT_CREATE " +
        "FROM SMC_MDM_SCCURVES WHERE HD_LOG_ID = ? ORDER BY DATE_READ, DATA_CLASS";

    public MdmImportService() {
        this.extractor = new LoadProfileDataExtractor();
    }

    // ========================================================================
    // Public Entry Points
    // ========================================================================

    /**
     * Process ZFA measurement data (main entry point for ZFA push)
     * 
     * @param xmlBody The raw XML payload
     * @param endpoint The endpoint URL that received the request
     * @param wsdlOperation The WSDL operation name (optional)
     * @return The HD_LOG_ID of the created header record
     */
    public long processZfaMeasurement(String xmlBody, String endpoint, String wsdlOperation)
            throws Exception {
        return processXmlPayload(xmlBody, endpoint, wsdlOperation,
            SOURCE_SYSTEM_ZFA, SOURCE_TYPE_MEASURE, null, null);
    }

    /**
     * Generic entry point for any source/type combination
     * 
     * @param xmlBody The raw XML payload
     * @param endpoint The endpoint URL
     * @param wsdlOperation WSDL operation name
     * @param sourceSystem ZFA or ITRON
     * @param sourceType MEASURE, ALARM, or EVENT
     * @param fileId File ID (for ITRON)
     * @param fileName File name (for ITRON)
     * @return The HD_LOG_ID of the created header record
     */
    public long processXmlPayload(String xmlBody, String endpoint, String wsdlOperation,
            String sourceSystem, String sourceType, String fileId, String fileName)
            throws Exception {

        ConnectOracleDAO dao = new ConnectOracleDAO();
        long hdLogId = 0;
        int recordsProcessed = 0;
        Connection conn = null;

        try {
            conn = dao.getConnection();
            conn.setAutoCommit(false);

            // 1. Extract metadata from XML
            XmlMetadata metadata = extractXmlMetadata(xmlBody);

            // 2. Insert header record (PENDING status)
            hdLogId = insertHeader(conn, sourceSystem, sourceType, fileId, fileName,
                metadata.messageUuid, wsdlOperation, endpoint,
                metadata.senderId, metadata.recipientId, metadata.creationDateTime, xmlBody);

            log.info("Created HD record {} for {} {} message UUID: {}",
                hdLogId, sourceSystem, sourceType, metadata.messageUuid);

            // 3. Parse XML and extract profiles
            List<LoadProfileData> profiles = extractor.extractFromXml(xmlBody);

            if (profiles.isEmpty()) {
                updateHeaderStatus(conn, hdLogId, "ERROR", "No profiles found in XML", 0);
                conn.commit();
                throw new Exception("XML parsed but contained no profiles");
            }

            // 4. Process each profile (channel)
            for (LoadProfileData profile : profiles) {
                List<CurveRow> curveRows = transformToCurveRows(profile, hdLogId,
                    sourceSystem, metadata.creationDateTime);

                for (CurveRow row : curveRows) {
                    insertCurveRow(conn, row);
                    recordsProcessed++;
                }
            }

            // 5. Update header to SUCCESS
            updateHeaderStatus(conn, hdLogId, "SUCCESS", null, recordsProcessed);
            conn.commit();

            log.info("Successfully processed {} curve records for HD_LOG_ID: {}",
                recordsProcessed, hdLogId);

            return hdLogId;

        } catch (Exception e) {
            log.error("Processing failed for HD_LOG_ID: {}", hdLogId, e);

            // Rollback current transaction
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.error("Rollback failed", ex);
                }
            }

            // Update header to ERROR status in new transaction
            if (hdLogId > 0) {
                try {
                    conn.setAutoCommit(true);
                    updateHeaderStatus(conn, hdLogId, "ERROR",
                        truncateMessage(e.getMessage(), 4000), recordsProcessed);
                } catch (Exception ex) {
                    log.error("Failed to update error status", ex);
                }
            }
            throw e;

        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.debug("Error closing connection", e);
                }
            }
        }
    }

    /**
     * Get log status by transaction ID (UUID)
     */
    public String getLogStatus(String txId) throws Exception {
        ConnectOracleDAO dao = new ConnectOracleDAO();
        try (Connection conn = dao.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_HD_BY_UUID_SQL)) {
            ps.setString(1, txId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : "NOT_FOUND";
            }
        }
    }

    /**
     * Get header record by LOG_ID
     */
    public Map<String, Object> getHeaderById(long logId) throws Exception {
        ConnectOracleDAO dao = new ConnectOracleDAO();
        try (Connection conn = dao.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_HD_SQL)) {

            ps.setLong(1, logId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Map<String, Object> result = new HashMap<>();
                    result.put("logId", rs.getLong("LOG_ID"));
                    result.put("sourceSystem", rs.getString("SOURCE_SYSTEM"));
                    result.put("sourceType", rs.getString("SOURCE_TYPE"));
                    result.put("messageUuid", rs.getString("MESSAGE_UUID"));
                    result.put("status", rs.getString("STATUS"));
                    result.put("statusMsg", rs.getString("STATUS_MSG"));
                    result.put("recordsProcessed", rs.getInt("RECORDS_PROCESSED"));
                    result.put("receivedAt", rs.getTimestamp("RECEIVED_AT"));
                    return result;
                }
                return null;
            }
        }
    }

    /**
     * Get processing summary for a header (list of curve records created)
     */
    public List<Map<String, Object>> getProcessingSummary(long logId) throws Exception {
        ConnectOracleDAO dao = new ConnectOracleDAO();
        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection conn = dao.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_CURVES_SUMMARY_SQL)) {

            ps.setLong(1, logId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("podId", rs.getString("POD_ID"));
                    row.put("supplyNum", rs.getString("SUPPLY_NUM"));
                    row.put("dataClass", rs.getString("DATA_CLASS"));
                    row.put("dateRead", rs.getDate("DATE_READ"));
                    row.put("sectionUuid", rs.getString("SECTION_UUID"));
                    row.put("sourceCreationDt", rs.getTimestamp("SOURCE_CREATION_DT"));
                    row.put("dtCreate", rs.getTimestamp("DT_CREATE"));
                    results.add(row);
                }
            }
        }
        return results;
    }

    // ========================================================================
    // Private Helper Methods
    // ========================================================================

    /**
     * Insert header record into SMC_MDM_SCCURVES_HD
     */
    private long insertHeader(Connection conn, String sourceSystem, String sourceType,
            String fileId, String fileName, String messageUuid, String wsdlOperation,
            String endpoint, String senderId, String recipientId,
            Timestamp sourceCreationDt, String rawXml) throws SQLException {

        String[] generatedColumns = {"LOG_ID"};
        try (PreparedStatement ps = conn.prepareStatement(INSERT_HD_SQL, generatedColumns)) {
            int idx = 1;
            ps.setString(idx++, sourceSystem);
            ps.setString(idx++, sourceType);
            ps.setString(idx++, fileId);
            ps.setString(idx++, fileName);
            ps.setString(idx++, messageUuid);
            ps.setString(idx++, wsdlOperation);
            ps.setString(idx++, endpoint);
            ps.setString(idx++, senderId);
            ps.setString(idx++, recipientId);
            ps.setTimestamp(idx++, sourceCreationDt);
            ps.setClob(idx++, new javax.sql.rowset.serial.SerialClob(
                rawXml != null ? rawXml.toCharArray() : new char[0]));

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to get generated LOG_ID");
    }

    /**
     * Update header status
     */
    private void updateHeaderStatus(Connection conn, long logId, String status,
            String statusMsg, int recordsProcessed) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_HD_STATUS_SQL)) {
            ps.setString(1, status);
            ps.setString(2, statusMsg);
            ps.setInt(3, recordsProcessed);
            ps.setLong(4, logId);
            ps.executeUpdate();
        }
    }

    /**
     * Transform LoadProfileData into CurveRows (horizontal pivot)
     * Groups intervals by Greek local date
     */
    private List<CurveRow> transformToCurveRows(LoadProfileData profile, long hdLogId,
            String sourceSystem, Timestamp sourceCreationDt) {

        Map<LocalDate, CurveRow> curvesByDate = new HashMap<>();

        String podIdFull = profile.getPodId();
        String supplyNum = extractSupplyNum(podIdFull);
        String dataClass = profile.getObisCode();
        String sectionUuid = profile.getMessageUuid();

        for (IntervalData interval : profile.getIntervals()) {
            LocalDateTime utcStart = interval.getStartDateTime();
            if (utcStart == null) continue;

            // Convert UTC to Greek local time
            ZonedDateTime utcZoned = utcStart.atZone(UTC_ZONE);
            ZonedDateTime greekZoned = utcZoned.withZoneSameInstant(GREEK_ZONE);
            LocalDate greekDate = greekZoned.toLocalDate();
            LocalDateTime greekLocalTime = greekZoned.toLocalDateTime();

            // Get or create curve row for this date
            CurveRow curve = curvesByDate.computeIfAbsent(greekDate, date -> {
                CurveRow newCurve = new CurveRow();
                newCurve.setHdLogId(hdLogId);
                newCurve.setSectionUuid(sectionUuid);
                newCurve.setPodId(podIdFull);
                newCurve.setSupplyNum(supplyNum);
                newCurve.setDateRead(date);
                newCurve.setDataClass(dataClass);
                newCurve.setUnitMeasure(interval.getUnitCode());
                newCurve.setSourceSystem(sourceSystem);
                newCurve.setSourceCreationDt(sourceCreationDt);
                return newCurve;
            });

            // Calculate Q index (1-based)
            // Q1 = 00:00-00:14, Q2 = 00:15-00:29, ... Q96 = 23:45-23:59
            int qIndex = (greekLocalTime.getHour() * 4) + (greekLocalTime.getMinute() / 15) + 1;

            if (qIndex >= 1 && qIndex <= MAX_INTERVALS) {
                curve.setQValue(qIndex, interval.getValue());
                curve.setSValue(qIndex, interval.getStatus());
            }
        }

        return new ArrayList<>(curvesByDate.values());
    }

    /**
     * Insert a curve row into SMC_MDM_SCCURVES
     */
    private void insertCurveRow(Connection conn, CurveRow row) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO SMC_MDM_SCCURVES (");
        sql.append("HD_LOG_ID, SECTION_UUID, POD_ID, SUPPLY_NUM, DATE_READ, DATA_CLASS, ");
        sql.append("UNIT_MEASURE, SOURCE_SYSTEM, SOURCE_CREATION_DT");

        // Add Q1-Q100 and S1-S100 columns
        for (int i = 1; i <= MAX_INTERVALS; i++) {
            sql.append(", Q").append(i);
            sql.append(", S").append(i);
        }
        sql.append(") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?");
        for (int i = 1; i <= MAX_INTERVALS * 2; i++) {
            sql.append(", ?");
        }
        sql.append(")");

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setLong(idx++, row.getHdLogId());
            ps.setString(idx++, row.getSectionUuid());
            ps.setString(idx++, row.getPodId());
            ps.setString(idx++, row.getSupplyNum());
            ps.setDate(idx++, java.sql.Date.valueOf(row.getDateRead()));
            ps.setString(idx++, row.getDataClass());
            ps.setString(idx++, row.getUnitMeasure());
            ps.setString(idx++, row.getSourceSystem());
            ps.setTimestamp(idx++, row.getSourceCreationDt());

            // Set Q and S values
            for (int i = 1; i <= MAX_INTERVALS; i++) {
                BigDecimal qVal = row.getQValue(i);
                if (qVal != null) {
                    ps.setBigDecimal(idx++, qVal);
                } else {
                    ps.setNull(idx++, Types.DECIMAL);
                }

                String sVal = row.getSValue(i);
                ps.setString(idx++, sVal);
            }

            ps.executeUpdate();
        }
    }

    /**
     * Extract supply number from POD ID
     * SUPPLY_NUM is 9 chars starting from position 4 (0-based index 3)
     */
    private String extractSupplyNum(String fullPod) {
        if (fullPod == null || fullPod.length() < 12) {
            return fullPod != null ? fullPod : "UNKNOWN";
        }
        return fullPod.substring(3, 12);
    }

    /**
     * Extract metadata from XML using regex (quick extraction without full parse)
     */
    private XmlMetadata extractXmlMetadata(String xml) {
        XmlMetadata metadata = new XmlMetadata();

        // Extract UUID
        metadata.messageUuid = extractTagValue(xml, "UUID");
        if (metadata.messageUuid == null) {
            metadata.messageUuid = UUID.randomUUID().toString().toUpperCase();
        }

        // Extract Sender ID
        metadata.senderId = extractNestedTagValue(xml, "SenderParty", "StandardID");

        // Extract Recipient ID
        metadata.recipientId = extractNestedTagValue(xml, "RecipientParty", "StandardID");

        // Extract CreationDateTime
        String creationDtStr = extractTagValue(xml, "CreationDateTime");
        if (creationDtStr != null) {
            try {
                Instant instant = Instant.parse(creationDtStr);
                metadata.creationDateTime = Timestamp.from(instant);
            } catch (Exception e) {
                metadata.creationDateTime = new Timestamp(System.currentTimeMillis());
            }
        } else {
            metadata.creationDateTime = new Timestamp(System.currentTimeMillis());
        }

        return metadata;
    }

    private String extractTagValue(String xml, String tagName) {
        try {
            String pattern = "<" + tagName + "[^>]*>([^<]+)</" + tagName + ">";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher m = p.matcher(xml);
            if (m.find()) {
                return m.group(1).trim();
            }
        } catch (Exception e) {
            log.debug("Could not extract {}", tagName);
        }
        return null;
    }

    private String extractNestedTagValue(String xml, String parentTag, String childTag) {
        try {
            String parentPattern = "<" + parentTag + "[^>]*>([\\s\\S]*?)</" + parentTag + ">";
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(parentPattern);
            java.util.regex.Matcher m = p.matcher(xml);
            if (m.find()) {
                String parentContent = m.group(1);
                return extractTagValue(parentContent, childTag);
            }
        } catch (Exception e) {
            log.debug("Could not extract {}/{}", parentTag, childTag);
        }
        return null;
    }

    private String truncateMessage(String msg, int maxLength) {
        if (msg == null) return null;
        return msg.length() > maxLength ? msg.substring(0, maxLength - 3) + "..." : msg;
    }

    // ========================================================================
    // Inner Classes
    // ========================================================================

    /**
     * Metadata extracted from XML header
     */
    private static class XmlMetadata {
        String messageUuid;
        String senderId;
        String recipientId;
        Timestamp creationDateTime;
    }

    /**
     * Represents a single curve row (one POD + one channel + one date)
     */
    public static class CurveRow {
        private long hdLogId;
        private String sectionUuid;
        private String podId;
        private String supplyNum;
        private LocalDate dateRead;
        private String dataClass;
        private String unitMeasure;
        private String sourceSystem;
        private Timestamp sourceCreationDt;

        private final Map<Integer, BigDecimal> qValues = new HashMap<>();
        private final Map<Integer, String> sValues = new HashMap<>();

        // Getters and setters
        public long getHdLogId() { return hdLogId; }
        public void setHdLogId(long hdLogId) { this.hdLogId = hdLogId; }

        public String getSectionUuid() { return sectionUuid; }
        public void setSectionUuid(String sectionUuid) { this.sectionUuid = sectionUuid; }

        public String getPodId() { return podId; }
        public void setPodId(String podId) { this.podId = podId; }

        public String getSupplyNum() { return supplyNum; }
        public void setSupplyNum(String supplyNum) { this.supplyNum = supplyNum; }

        public LocalDate getDateRead() { return dateRead; }
        public void setDateRead(LocalDate dateRead) { this.dateRead = dateRead; }

        public String getDataClass() { return dataClass; }
        public void setDataClass(String dataClass) { this.dataClass = dataClass; }

        public String getUnitMeasure() { return unitMeasure; }
        public void setUnitMeasure(String unitMeasure) { this.unitMeasure = unitMeasure; }

        public String getSourceSystem() { return sourceSystem; }
        public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

        public Timestamp getSourceCreationDt() { return sourceCreationDt; }
        public void setSourceCreationDt(Timestamp sourceCreationDt) {
            this.sourceCreationDt = sourceCreationDt;
        }

        public void setQValue(int index, BigDecimal value) {
            qValues.put(index, value);
        }

        public BigDecimal getQValue(int index) {
            return qValues.get(index);
        }

        public void setSValue(int index, String value) {
            sValues.put(index, value);
        }

        public String getSValue(int index) {
            return sValues.get(index);
        }
    }
}
