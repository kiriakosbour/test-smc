package com.hedno.integration.service;

import com.hedno.integration.dao.ConnectOracleDAO; // Using the DAO from your files
import com.hedno.integration.processor.IntervalData;
import com.hedno.integration.processor.LoadProfileData;
import com.hedno.integration.processor.LoadProfileDataExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class MdmImportService {

    private static final Logger log = LoggerFactory.getLogger(MdmImportService.class);
    private final LoadProfileDataExtractor extractor;

    // SQL Constants
    private static final String INSERT_LOG_SQL = "INSERT INTO SMC_MDM_DEBUG_LOG (SOURCE_SYSTEM, ENDPOINT, TRANSACTION_ID, STATUS, PAYLOAD_XML) VALUES (?, ?, ?, ?, ?)";

    private static final String UPDATE_LOG_STATUS_SQL = "UPDATE SMC_MDM_DEBUG_LOG SET STATUS = ?, ERROR_MSG = ? WHERE TRANSACTION_ID = ?";

    private static final String MERGE_HEADER_SQL = "MERGE INTO SMC_MDM_DATA_HD h " +
            "USING (SELECT ? as pod, ? as dclass, ? as logid FROM dual) s " +
            "ON (h.POD_ID = s.pod AND h.DATA_CLASS = s.dclass AND h.DEBUG_LOG_ID = s.logid) " +
            "WHEN NOT MATCHED THEN INSERT (DEBUG_LOG_ID, POD_ID, SUPPLY_NUM, DATA_CLASS, SOURCE_SYSTEM) " +
            "VALUES (?, ?, ?, ?, ?)";

    private static final String GET_HEADER_ID_SQL = "SELECT ID FROM SMC_MDM_DATA_HD WHERE POD_ID = ? AND DATA_CLASS = ? AND DEBUG_LOG_ID = ?";

    public MdmImportService() {
        this.extractor = new LoadProfileDataExtractor();
    }

    public String getLogStatus(String txId) throws Exception {
        Properties props = new Properties(); // Populate with actual config
        ConnectOracleDAO dao = new ConnectOracleDAO(props);
        try (Connection conn = dao.getConnection();
                PreparedStatement ps = conn
                        .prepareStatement("SELECT STATUS FROM SMC_MDM_DEBUG_LOG WHERE TRANSACTION_ID = ?")) {
            ps.setString(1, txId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : "NOT_FOUND";
            }
        }
    }

    public void processIncomingXml(String txId, String xmlBody) throws Exception {
        Properties props = new Properties(); // Ensure these are loaded from your config file
        // props.setProperty("db.url", ...);

        ConnectOracleDAO dao = new ConnectOracleDAO(props);
        long debugLogId = 0;

        try (Connection conn = dao.getConnection()) {
            if (conn == null)
                throw new SQLException("Could not establish DB connection");
            conn.setAutoCommit(false);

            // 1. INITIAL LOG (PENDING)
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO SMC_MDM_DEBUG_LOG (SOURCE_SYSTEM, ENDPOINT, TRANSACTION_ID, STATUS, PAYLOAD_XML, SENDER_ID) VALUES (?, ?, ?, ?, ?, ?)")) { // Added
                                                                                                                                                            // SENDER_ID

                ps.setString(1, "ZFA_MDM");
                ps.setString(2, "/mdm/push");
                ps.setString(3, txId);
                ps.setString(4, "PENDING");
                ps.setClob(5, new javax.sql.rowset.serial.SerialClob(xmlBody.toCharArray()));

                // Quick extraction of Sender ID without re-parsing the whole DOM yet
                String senderId = extractSenderIdRegex(xmlBody);
                ps.setString(6, senderId);

                ps.executeUpdate();

                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next())
                        debugLogId = rs.getLong(1);
                }
            }
            conn.commit(); // Commit log immediately so we have a record even if processing fails

            // 2. PARSE XML
            List<LoadProfileData> profiles = extractor.extractFromXml(xmlBody);

            if (profiles.isEmpty()) {
                throw new Exception(
                        "XML Parsed successfully but contained no UtilitiesTimeSeriesERPItemNotificationMessage");
            }

            // 3. PROCESS EACH PROFILE (Channel)
            for (LoadProfileData profile : profiles) {
                processSingleProfile(conn, profile, debugLogId);
            }

            // 4. UPDATE LOG (SUCCESS)
            updateLogStatus(conn, txId, "SUCCESS", null);
            conn.commit();

        } catch (Exception e) {
            // Handle Rollback and Error Logging
            log.error("Transaction failed", e);
            try (Connection conn = dao.getConnection()) {
                updateLogStatus(conn, txId, "ERROR", e.getMessage());
                conn.commit();
            }
            throw e;
        }
    }

    private void updateLogStatus(Connection conn, String txId, String status, String error) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPDATE_LOG_STATUS_SQL)) {
            ps.setString(1, status);
            ps.setString(2, error != null && error.length() > 4000 ? error.substring(0, 4000) : error);
            ps.setString(3, txId);
            ps.executeUpdate();
        }
    }

    private String extractSenderIdRegex(String xml) {
        try {
            // Simple regex to find <StandardID> inside <SenderParty>
            // Note: Robust XML parsing is better, but this suffices for the log header
            java.util.regex.Pattern p = java.util.regex.Pattern
                    .compile("<SenderParty>\\s*<StandardID[^>]*>(.*?)</StandardID>");
            java.util.regex.Matcher m = p.matcher(xml);
            if (m.find())
                return m.group(1);
        } catch (Exception e) {
            /* ignore */ }
        return "UNKNOWN";
    }

    private void processSingleProfile(Connection conn, LoadProfileData profile, long debugLogId) throws Exception {
        String podIdFull = profile.getPodId(); // 22 chars
        String obisCode = profile.getObisCode(); // e.g., 1-11:1.5.0

        // Requirement: SUPPLY_NUM is 9 chars starting from index 4 of POD_ID
        String supplyNum = extractSupplyNum(podIdFull);

        // Requirement: DATA_CLASS is the Obis Code
        String dataClass = obisCode;

        // 1. Ensure Header Exists (SMC_MDM_DATA_HD)
        ensureDataHeader(conn, debugLogId, podIdFull, supplyNum, dataClass);
        long headerId = getDataHeaderId(conn, debugLogId, podIdFull, dataClass);

        // 2. Transform Vertical Intervals to Horizontal Daily Curves
        Map<LocalDate, DailyCurve> curves = transformToCurves(profile.getIntervals());

        // 3. Insert Curves (SMC_MDM_SCCURVES)
        for (DailyCurve curve : curves.values()) {
            insertCurveRow(conn, headerId, podIdFull, supplyNum, dataClass, curve);
        }
    }

    private void ensureDataHeader(Connection conn, long logId, String pod, String supply, String dClass)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(MERGE_HEADER_SQL)) {
            // MERGE Using vals
            ps.setString(1, pod);
            ps.setString(2, dClass);
            ps.setLong(3, logId);
            // INSERT vals
            ps.setLong(4, logId);
            ps.setString(5, pod);
            ps.setString(6, supply);
            ps.setString(7, dClass);
            ps.setString(8, "ZFA");
            ps.executeUpdate();
        }
    }

    private long getDataHeaderId(Connection conn, long logId, String pod, String dClass) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(GET_HEADER_ID_SQL)) {
            ps.setString(1, pod);
            ps.setString(2, dClass);
            ps.setLong(3, logId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getLong(1);
            }
        }
        throw new SQLException("Header not found after merge. Logic Error.");
    }

    private String extractSupplyNum(String fullPod) {
        if (fullPod == null)
            return "UNKNOWN";

        // REQUIREMENT: "Start from position 4".
        // In Java (0-based), Position 1 is Index 0. Position 4 is Index 3.
        // We take 9 characters from there.
        if (fullPod.length() >= 12) {
            return fullPod.substring(3, 12); // indices 3 to 12 (exclusive) = 9 chars
        }
        return fullPod;
    }

    // --- CRITICAL: TRANSFORMATION LOGIC ---

    private void insertCurveRow(Connection conn, long headerId, String pod, String supply, String dClass,
            DailyCurve curve) throws SQLException {
        StringBuilder sql = new StringBuilder(
                "INSERT INTO SMC_MDM_SCCURVES (HEADER_ID, POD_ID, SUPPLY_NUM, DATE_READ, DATA_CLASS, UNIT_MEASURE, SOURCE_SYSTEM");

        // Add Q1..Q100 and S1..S100 dynamically to SQL
        for (int i = 1; i <= 100; i++) {
            sql.append(", Q").append(i);
            sql.append(", S").append(i);
        }
        sql.append(") VALUES (?, ?, ?, ?, ?, ?, ?"); // Base 7 params
        for (int i = 1; i <= 200; i++)
            sql.append(", ?"); // Q+S params
        sql.append(")");

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setLong(idx++, headerId);
            ps.setString(idx++, pod);
            ps.setString(idx++, supply);
            ps.setDate(idx++, java.sql.Date.valueOf(curve.date));
            ps.setString(idx++, dClass);
            ps.setString(idx++, curve.unitMeasure);
            ps.setString(idx++, "ZFA");

            // Fill Q1-Q100 and S1-S100
            for (int i = 1; i <= 100; i++) {
                ps.setBigDecimal(idx++, curve.qValues.getOrDefault(i, null));
                ps.setString(idx++, curve.sValues.getOrDefault(i, null));
            }
            ps.executeUpdate();
        }
    }

    /**
     * Groups arbitrary intervals into Daily Curves.
     * Handles the "vertical to horizontal" pivot.
     */
    private Map<LocalDate, DailyCurve> transformToCurves(List<IntervalData> intervals) {
        Map<LocalDate, DailyCurve> map = new HashMap<>();

        for (IntervalData interval : intervals) {
            LocalDateTime start = interval.getStartDateTime();
            if (start == null)
                continue;

            LocalDate date = start.toLocalDate();
            map.putIfAbsent(date, new DailyCurve(date));
            DailyCurve curve = map.get(date);

            // Determine Q Index (1-based)
            // 00:00 -> Q1 (Standard implementation) or 00:15 -> Q1?
            // Requirements imply Q1 is the first quarter.
            // Formula: (Hour * 4) + (Minute / 15) + 1
            // 00:00 = 1, 00:15 = 2 ... 23:45 = 96.
            int qIndex = (start.getHour() * 4) + (start.getMinute() / 15) + 1;

            if (qIndex >= 1 && qIndex <= 100) {
                curve.qValues.put(qIndex, interval.getValue());
                curve.sValues.put(qIndex, interval.getStatus());
                if (curve.unitMeasure == null)
                    curve.unitMeasure = interval.getUnitCode();
            }
        }
        return map;
    }

    // Helper Class for Horizontal Row
    private static class DailyCurve {
        LocalDate date;
        String unitMeasure;
        Map<Integer, BigDecimal> qValues = new HashMap<>();
        Map<Integer, String> sValues = new HashMap<>();

        DailyCurve(LocalDate date) {
            this.date = date;
        }
    }
}