package com.hedno.integration.service;

import com.hedno.integration.dao.ConnectOracleDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * XML Reader Service for processing ITRON SFTP files.
 * Handles readings, alarms, and events XML files.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class XMLReaderService {

    private static final Logger logger = LoggerFactory.getLogger(XMLReaderService.class);
    
    // Configuration
    private static final Properties properties = new Properties();
    
    // Constants
    private static final int BATCH_SIZE = 4096;
    private static final Pattern ATTR_NO_QUOTES = Pattern.compile("(\\s+[\\w:]+)=([^\"'\\s>]+)");
    private static final String ITRON_NAMESPACE = "http://www.itron.com/ItronInternalXsd/1.0/";
    
    // SQL Statements
    private static final String SELECT_SQL_PROCESS = "SELECT F_ID FROM ITRON_FILE_PROCESS WHERE F_NAME = ?";
    private static final String INSERT_SQL_PROCESS = "INSERT INTO ITRON_FILE_PROCESS (F_NAME) VALUES (?)";
    private static final String INSERT_SQL_READINGS = "INSERT INTO ITRON_FILE_READINGS (F_ID, SERV_POINT_CHANNEL, METER_VALUE, STATUS_REF, READING_TIME) VALUES (?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_ALARMS = "INSERT INTO ITRON_FILE_ALARMS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME, IS_HISTORICAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_EVENTS = "INSERT INTO ITRON_FILE_EVENTS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME) VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_SQL_PROCESS = "UPDATE ITRON_FILE_PROCESS SET PROCESS_RESULT = ?, PROCESS_MESSAGE = ? WHERE F_ID = ?";

    static {
        try (InputStream input = XMLReaderService.class.getClassLoader()
                .getResourceAsStream("config/application.properties")) {
            if (input == null) {
                throw new RuntimeException("Cannot find application.properties on classpath");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading application.properties", e);
        }
    }

    /**
     * Get property value
     */
    public static String get(String key) {
        return properties.getProperty(key);
    }

    // ========================================================================
    // Process Events XML
    // ========================================================================

    /**
     * Process Events XML file from ITRON SFTP
     * 
     * @param xmlFilePath Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processEventsXML(String xmlFilePath) {
        long startTime = System.currentTimeMillis();
        long fileId = 0;

        ConnectOracleDAO dao = new ConnectOracleDAO(properties);
        Connection conn = null;
        PreparedStatement psProcess = null;
        PreparedStatement psSelect = null;
        PreparedStatement psEvents = null;
        PreparedStatement psUpdate = null;

        try {
            conn = dao.getConnection();
            conn.setAutoCommit(false);

            psProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            psSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            psEvents = conn.prepareStatement(INSERT_SQL_EVENTS);
            psUpdate = conn.prepareStatement(UPDATE_SQL_PROCESS);

            // Insert file process record
            psProcess.setString(1, xmlFilePath);
            psProcess.executeUpdate();

            // Get generated file ID
            psSelect.setString(1, xmlFilePath);
            try (ResultSet rs = psSelect.executeQuery()) {
                if (rs.next()) {
                    fileId = rs.getLong(1);
                }
            }

            // Parse XML
            File xmlFile = new File(xmlFilePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            logger.info("Processing events XML: {}, Root: {}", 
                xmlFilePath, doc.getDocumentElement().getNodeName());

            // Process Event elements
            NodeList eventNodes = doc.getElementsByTagName("Event");
            logger.info("Total events found: {}", eventNodes.getLength());

            int batchCount = 0;
            for (int i = 0; i < eventNodes.getLength(); i++) {
                Node eventNode = eventNodes.item(i);
                if (eventNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eventEl = (Element) eventNode;

                    psEvents.setLong(1, fileId);
                    psEvents.setString(2, getChildText(eventEl, "CollectionSystemID"));
                    psEvents.setString(3, getChildText(eventEl, "ObjectID"));
                    psEvents.setString(4, getChildText(eventEl, "ObjectType"));
                    psEvents.setString(5, getChildText(eventEl, "EventType"));
                    psEvents.setString(6, getChildText(eventEl, "EventDateTime"));
                    psEvents.setString(7, getChildText(eventEl, "CaptureDateTime"));
                    psEvents.addBatch();

                    if (++batchCount >= BATCH_SIZE) {
                        psEvents.executeBatch();
                        psEvents.clearBatch();
                        batchCount = 0;
                    }
                }
            }

            // Execute remaining batch
            if (batchCount > 0) {
                psEvents.executeBatch();
            }

            // Update process status to success
            psUpdate.setLong(1, 0L);
            psUpdate.setString(2, null);
            psUpdate.setLong(3, fileId);
            psUpdate.executeUpdate();

            conn.commit();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Events XML processed successfully - File: {}, Events: {}, Duration: {}ms",
                xmlFilePath, eventNodes.getLength(), duration);

            return 0;

        } catch (Exception e) {
            logger.error("Error processing events XML: {}", xmlFilePath, e);
            rollbackQuietly(conn);
            updateProcessError(psUpdate, fileId, e.getMessage());
            commitQuietly(conn);
            return -1;

        } finally {
            closeQuietly(psEvents);
            closeQuietly(psSelect);
            closeQuietly(psProcess);
            closeQuietly(psUpdate);
            closeQuietly(conn);
        }
    }

    // ========================================================================
    // Process Alarms XML
    // ========================================================================

    /**
     * Process Alarms XML file from ITRON SFTP
     * 
     * @param xmlFilePath Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processAlarmsXML(String xmlFilePath) {
        long startTime = System.currentTimeMillis();
        long fileId = 0;

        ConnectOracleDAO dao = new ConnectOracleDAO(properties);
        Connection conn = null;
        PreparedStatement psProcess = null;
        PreparedStatement psSelect = null;
        PreparedStatement psAlarms = null;
        PreparedStatement psUpdate = null;

        try {
            conn = dao.getConnection();
            conn.setAutoCommit(false);

            psProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            psSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            psAlarms = conn.prepareStatement(INSERT_SQL_ALARMS);
            psUpdate = conn.prepareStatement(UPDATE_SQL_PROCESS);

            // Insert file process record
            psProcess.setString(1, xmlFilePath);
            psProcess.executeUpdate();

            // Get generated file ID
            psSelect.setString(1, xmlFilePath);
            try (ResultSet rs = psSelect.executeQuery()) {
                if (rs.next()) {
                    fileId = rs.getLong(1);
                }
            }

            // Parse XML with namespace awareness
            File xmlFile = new File(xmlFilePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            logger.info("Processing alarms XML: {}", xmlFilePath);

            // Process Event elements with namespace
            NodeList eventList = doc.getElementsByTagNameNS(ITRON_NAMESPACE, "Event");
            logger.info("Total alarms found: {}", eventList.getLength());

            int batchCount = 0;
            for (int i = 0; i < eventList.getLength(); i++) {
                Element event = (Element) eventList.item(i);

                psAlarms.setLong(1, fileId);
                psAlarms.setString(2, getTextNS(event, ITRON_NAMESPACE, "CollectionSystemID"));
                psAlarms.setString(3, getTextNS(event, ITRON_NAMESPACE, "ObjectID"));
                psAlarms.setString(4, getTextNS(event, ITRON_NAMESPACE, "ObjectType"));
                psAlarms.setString(5, getTextNS(event, ITRON_NAMESPACE, "EventType"));
                psAlarms.setString(6, getTextNS(event, ITRON_NAMESPACE, "EventDateTime"));
                psAlarms.setString(7, getTextNS(event, ITRON_NAMESPACE, "CaptureDateTime"));
                psAlarms.setString(8, getTextNS(event, ITRON_NAMESPACE, "IsHistorical"));
                psAlarms.addBatch();

                if (++batchCount >= BATCH_SIZE) {
                    psAlarms.executeBatch();
                    psAlarms.clearBatch();
                    batchCount = 0;
                }
            }

            // Execute remaining batch
            if (batchCount > 0) {
                psAlarms.executeBatch();
            }

            // Update process status to success
            psUpdate.setLong(1, 0L);
            psUpdate.setString(2, null);
            psUpdate.setLong(3, fileId);
            psUpdate.executeUpdate();

            conn.commit();

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Alarms XML processed successfully - File: {}, Alarms: {}, Duration: {}ms",
                xmlFilePath, eventList.getLength(), duration);

            return 0;

        } catch (Exception e) {
            logger.error("Error processing alarms XML: {}", xmlFilePath, e);
            rollbackQuietly(conn);
            updateProcessError(psUpdate, fileId, e.getMessage());
            commitQuietly(conn);
            return -1;

        } finally {
            closeQuietly(psAlarms);
            closeQuietly(psSelect);
            closeQuietly(psProcess);
            closeQuietly(psUpdate);
            closeQuietly(conn);
        }
    }

    // ========================================================================
    // Process Readings XML
    // ========================================================================

    /**
     * Process Readings XML file from ITRON SFTP
     * 
     * Steps:
     * 1. Validate XML Structure
     * 2. If specific validation error -> Fix file (missing quotes)
     * 3. Load valid XML into database
     * 
     * @param xmlFilePath Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processReadingsXML(String xmlFilePath) {
        long startTime = System.currentTimeMillis();
        long fileId = 0;
        String workingFilePath = xmlFilePath;
        String fixedFilePath = "READINGS_template_fixed.xml";

        ConnectOracleDAO dao = new ConnectOracleDAO(properties);
        Connection conn = null;
        PreparedStatement psProcess = null;
        PreparedStatement psSelect = null;
        PreparedStatement psReadings = null;
        PreparedStatement psUpdate = null;

        try {
            conn = dao.getConnection();
            conn.setAutoCommit(false);

            psProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            psSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            psReadings = conn.prepareStatement(INSERT_SQL_READINGS);
            psUpdate = conn.prepareStatement(UPDATE_SQL_PROCESS);

            // Insert file process record
            psProcess.setString(1, xmlFilePath);
            psProcess.executeUpdate();

            // Get generated file ID
            psSelect.setString(1, xmlFilePath);
            try (ResultSet rs = psSelect.executeQuery()) {
                if (rs.next()) {
                    fileId = rs.getLong(1);
                }
            }

            // Validate XML structure
            String validationResult = validateReadingsXML(workingFilePath);
            String[] validationParts = validationResult.split("#", 2);
            String errorCode = validationParts[0];
            String errorMsg = validationParts.length > 1 ? validationParts[1] : "";

            logger.debug("XML validation result: code={}, msg={}", errorCode, errorMsg);

            // Try to fix if it's a quote issue
            if ("1".equals(errorCode)) {
                int fixResult = fixQuotes(workingFilePath, fixedFilePath);
                if (fixResult == 1) {
                    workingFilePath = fixedFilePath;
                    errorCode = "0"; // Mark as fixed
                } else {
                    throw new Exception("Failed to fix XML quotes: " + errorMsg);
                }
            }

            // If still not valid, fail
            if (!"0".equals(errorCode)) {
                throw new Exception("XML validation failed: " + errorMsg);
            }

            // Parse and load valid XML
            File xmlFile = new File(workingFilePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setIgnoringComments(true);
            factory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            logger.info("Processing readings XML: {}, Root: {}", 
                xmlFilePath, doc.getDocumentElement().getNodeName());

            // Process Channels
            NodeList channels = doc.getElementsByTagName("Channel");
            logger.info("Channels found: {}", channels.getLength());

            int totalReadings = 0;
            for (int i = 0; i < channels.getLength(); i++) {
                Element channel = (Element) channels.item(i);
                String servicePoint = channel.getElementsByTagName("ChannelID")
                    .item(0).getAttributes().getNamedItem("ServicePointChannelID").getTextContent();

                // Process Readings for this channel
                NodeList readings = channel.getElementsByTagName("Reading");
                int batchCount = 0;

                for (int j = 0; j < readings.getLength(); j++) {
                    Element reading = (Element) readings.item(j);
                    String value = reading.getAttribute("Value");
                    String statusRef = reading.getAttribute("StatusRef");
                    String readingTime = reading.hasAttribute("ReadingTime") 
                        ? reading.getAttribute("ReadingTime") : null;

                    psReadings.setLong(1, fileId);
                    psReadings.setString(2, servicePoint);
                    psReadings.setDouble(3, Double.parseDouble(value));
                    psReadings.setString(4, statusRef);
                    psReadings.setString(5, readingTime);
                    psReadings.addBatch();

                    if (++batchCount >= BATCH_SIZE) {
                        psReadings.executeBatch();
                        psReadings.clearBatch();
                        batchCount = 0;
                    }
                    totalReadings++;
                }

                // Execute remaining batch for this channel
                if (batchCount > 0) {
                    psReadings.executeBatch();
                    psReadings.clearBatch();
                }
            }

            // Update process status to success
            psUpdate.setLong(1, 0L);
            psUpdate.setString(2, null);
            psUpdate.setLong(3, fileId);
            psUpdate.executeUpdate();

            conn.commit();

            // Cleanup fixed file if created
            if (!workingFilePath.equals(xmlFilePath)) {
                new File(fixedFilePath).delete();
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Readings XML processed successfully - File: {}, Channels: {}, Readings: {}, Duration: {}ms",
                xmlFilePath, channels.getLength(), totalReadings, duration);

            return 0;

        } catch (Exception e) {
            logger.error("Error processing readings XML: {}", xmlFilePath, e);
            rollbackQuietly(conn);
            updateProcessError(psUpdate, fileId, e.getMessage());
            commitQuietly(conn);
            return -1;

        } finally {
            closeQuietly(psReadings);
            closeQuietly(psSelect);
            closeQuietly(psProcess);
            closeQuietly(psUpdate);
            closeQuietly(conn);
        }
    }

    // ========================================================================
    // XML Validation and Repair
    // ========================================================================

    /**
     * Validate XML structure
     * 
     * @param xmlFilePath Path to XML file
     * @return Error code and message separated by #
     *         "0#" = valid
     *         "1#..." = fixable quote error
     *         "2#..." to "5#..." = other errors
     *         "-1#..." = exception
     */
    public static String validateReadingsXML(String xmlFilePath) {
        File xmlFile = new File(xmlFilePath);

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setValidating(false);
            factory.setIgnoringComments(true);
            factory.setIgnoringElementContentWhitespace(true);

            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new ErrorHandler() {
                @Override
                public void warning(SAXParseException e) {
                    logger.warn("XML Warning: {}", e.getMessage());
                }

                @Override
                public void error(SAXParseException e) throws SAXException {
                    logger.error("XML Error: {}", e.getMessage());
                    throw e;
                }

                @Override
                public void fatalError(SAXParseException e) throws SAXException {
                    logger.error("XML Fatal: {}", e.getMessage());
                    throw e;
                }
            });

            builder.parse(xmlFile);
            return "0#";

        } catch (SAXParseException e) {
            String msg = e.getMessage();
            if (msg.contains("Open quote is expected for attribute")) {
                return "1#" + msg;
            }
            if (msg.contains("must not contain the")) {
                return "2#" + msg;
            }
            if (msg.contains("must be terminated by the matching end-tag")) {
                return "3#" + msg;
            }
            if (msg.contains("must be followed by either attribute specifications")) {
                return "4#" + msg;
            }
            return "5#" + msg;

        } catch (Exception e) {
            logger.error("XML validation exception", e);
            return "-1#" + e.getMessage();
        }
    }

    /**
     * Fix unquoted attributes in XML
     * 
     * @param inputPath Input XML file path
     * @param outputPath Output fixed XML file path
     * @return 1 on success, -1 on error
     */
    public static int fixQuotes(String inputPath, String outputPath) {
        logger.debug("Fixing quotes - Input: {}, Output: {}", inputPath, outputPath);

        try (Scanner scanner = new Scanner(new File(inputPath), "UTF-8");
             PrintWriter writer = new PrintWriter(outputPath, "UTF-8")) {

            StringBuilder content = new StringBuilder();
            while (scanner.hasNextLine()) {
                content.append(scanner.nextLine()).append(System.lineSeparator());
            }

            String xmlText = content.toString();

            // Add quotes around unquoted attribute values
            Matcher m = ATTR_NO_QUOTES.matcher(xmlText);
            StringBuffer sb = new StringBuffer();
            while (m.find()) {
                String attr = m.group(1);
                String val = m.group(2);
                m.appendReplacement(sb, attr + "=\"" + val + "\"");
            }
            m.appendTail(sb);

            writer.print(sb.toString());
            logger.info("Repaired XML written to: {}", outputPath);

            // Verify the repaired file can be parsed
            Path fixed = Paths.get(outputPath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(fixed.toFile());
            logger.info("Fixed XML validated successfully. Root: {}", 
                doc.getDocumentElement().getNodeName());

            return 1;

        } catch (Exception e) {
            logger.error("Error fixing quotes", e);
            return -1;
        }
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /**
     * Get child element text content (no namespace)
     */
    private static String getChildText(Element parent, String tagName) {
        NodeList list = parent.getElementsByTagName(tagName);
        if (list.getLength() == 0) {
            return null;
        }
        return list.item(0).getTextContent();
    }

    /**
     * Get element text content with namespace
     */
    private static String getTextNS(Element parent, String ns, String tagName) {
        NodeList nl = parent.getElementsByTagNameNS(ns, tagName);
        return (nl != null && nl.getLength() > 0) ? nl.item(0).getTextContent() : "";
    }

    /**
     * Update process record with error
     */
    private static void updateProcessError(PreparedStatement ps, long fileId, String errorMsg) {
        if (ps == null || fileId == 0) return;
        try {
            ps.setLong(1, -1L);
            ps.setString(2, truncate(errorMsg, 4000));
            ps.setLong(3, fileId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to update process error status", e);
        }
    }

    /**
     * Truncate string to max length
     */
    private static String truncate(String str, int maxLen) {
        if (str == null) return null;
        return str.length() > maxLen ? str.substring(0, maxLen - 3) + "..." : str;
    }

    /**
     * Close PreparedStatement quietly
     */
    private static void closeQuietly(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.debug("Error closing PreparedStatement", e);
            }
        }
    }

    /**
     * Close Connection quietly
     */
    private static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.debug("Error closing Connection", e);
            }
        }
    }

    /**
     * Rollback quietly
     */
    private static void rollbackQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                logger.debug("Error rolling back", e);
            }
        }
    }

    /**
     * Commit quietly
     */
    private static void commitQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                logger.debug("Error committing", e);
            }
        }
    }
}
