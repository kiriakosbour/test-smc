package com.hedno.integration.service;

import com.hedno.integration.dao.ConnectOracleDAO;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintWriter;
import org.w3c.dom.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Reading XML data from SFTP Server.
 * This is used when we need to parse XML data to store into the database.
 *
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class XMLReaderService {
    private static final Logger logger = LoggerFactory.getLogger(XMLReaderService.class);
    private static final Properties properties = new Properties();

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

    private static final Pattern ATTR_NO_QUOTES = Pattern.compile("(\\s+[\\w:]+)=([^\"'\\s>]+)");
    private static final String SELECT_SQL_PROCESS = "SELECT F_ID FROM ITRON_FILE_PROCESS where F_NAME = ?";
    private static final String INSERT_SQL_PROCESS = "INSERT INTO ITRON_FILE_PROCESS (F_NAME) values (?)";
    private static final String INSERT_SQL_READINGS = "INSERT INTO ITRON_FILE_READINGS (F_ID, SERV_POINT_CHANNEL, METER_VALUE, STATUS_REF, READING_TIME) values (?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_ALARMS = "INSERT INTO ITRON_FILE_ALARMS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME, IS_HISTORICAL) values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_EVENTS = "INSERT INTO ITRON_FILE_EVENTS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME) values (?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_SQL_PROCESS = "UPDATE ITRON_FILE_PROCESS SET PROCESS_RESULT = ?, PROCESS_MESSAGE = ? WHERE F_ID = ?";

    public static String get(String key) {
        return properties.getProperty(key);
    }

    /**
     * Process Events XML file from ITRON SFTP
     * 
     * @param in_xmlFile Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processEventsXML(String in_xmlFile) {

        ConnectOracleDAO objConn = new ConnectOracleDAO(properties);
        Connection conn = null;

        String xmlFilePath = in_xmlFile;
        String xmlFilePathFixed = "events_fixed.xml";
        String erroCoderMessg = null;
        String regex = "[#]";
        Integer errorRepair = 999;
        long startTime = System.currentTimeMillis();
        long totalDuration;
        File xmlFile = new File(in_xmlFile);
        long fileId = 0;

        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementEventsData = null;
        
        try {
            // Get connection - now throws Exception
            conn = objConn.getConnection();
            if (conn == null) {
                logger.error("Failed to get database connection");
                return -1;
            }

            preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            preparedStatementEventsData = conn.prepareStatement(INSERT_SQL_EVENTS);
            psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);

            // Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            // Retrieve Id generated for Detailed Table
            preparedStatementSelect.setString(1, in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();

            while (rs.next()) {
                fileId = rs.getLong(1);
            }
            rs.close();

            // 1. Create a DocumentBuilderFactory
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);

            // 2. Build the document
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            logger.info("Root element: {}", doc.getDocumentElement().getNodeName());

            // 3. Get all <Event> elements
            NodeList eventNodes = doc.getElementsByTagName("Event");
            logger.info("Total events: {}", eventNodes.getLength());

            int batchTotal = 0;
            for (int i = 0; i < eventNodes.getLength(); i++) {
                Node eventNode = eventNodes.item(i);
                if (eventNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eventEl = (Element) eventNode;

                    String collectionSystemId = getChildText(eventEl, "CollectionSystemID");
                    String objectId = getChildText(eventEl, "ObjectID");
                    String objectType = getChildText(eventEl, "ObjectType");
                    String eventType = getChildText(eventEl, "EventType");
                    String eventDateTime = getChildText(eventEl, "EventDateTime");
                    String captureDateTime = getChildText(eventEl, "CaptureDateTime");

                    logger.debug("Event #{} - ObjectID: {}, EventType: {}", 
                        (i + 1), objectId, eventType);

                    // Insert into ITRON_FILE_EVENTS
                    preparedStatementEventsData.setLong(1, fileId);
                    preparedStatementEventsData.setString(2, collectionSystemId);
                    preparedStatementEventsData.setString(3, objectId);
                    preparedStatementEventsData.setString(4, objectType);
                    preparedStatementEventsData.setString(5, eventType);
                    preparedStatementEventsData.setString(6, eventDateTime);
                    preparedStatementEventsData.setString(7, captureDateTime);
                    preparedStatementEventsData.addBatch();
                    
                    if (batchTotal++ == 4196) {
                        int[] resultData = preparedStatementEventsData.executeBatch();
                        preparedStatementEventsData.clearBatch();
                        batchTotal = 0;
                    }
                }
            }
            
            // Insert rest of records
            if (batchTotal > 0) {
                int[] resultData = preparedStatementEventsData.executeBatch();
            }
            
            psUpdProcess.setLong(1, 0L);
            psUpdProcess.setString(2, null);
            psUpdProcess.setLong(3, fileId);
            psUpdProcess.addBatch();
            int[] resultData = psUpdProcess.executeBatch();
            
            totalDuration = System.currentTimeMillis() - startTime;
            logger.info("Events processing completed - Duration: {}ms", totalDuration);
            return 0;

        } catch (Exception e) {
            logger.info("for update fileId: {}", fileId);
            logger.error("Error processing events XML: {}", e.getMessage(), e);

            try {
                if (psUpdProcess != null && fileId > 0) {
                    psUpdProcess.setLong(1, -1L);
                    psUpdProcess.setString(2, e.getMessage());
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
            } catch (Exception ex) {
                logger.error("Failed to update process status", ex);
            }
            return -1;
        } finally {
            closeQuietly(preparedStatementEventsData);
            closeQuietly(preparedStatementSelect);
            closeQuietly(preparedStatementProcess);
            closeQuietly(psUpdProcess);
            closeQuietly(conn);
        }
    }

    /**
     * Convenience method: returns the text content of the first child element
     * with the given tag name, or null if not present.
     */
    private static String getChildText(Element parent, String tagName) {
        NodeList list = parent.getElementsByTagName(tagName);
        if (list.getLength() == 0) {
            return null;
        }
        Node node = list.item(0);
        return node.getTextContent();
    }

    /**
     * Process Alarms XML file from ITRON SFTP
     * 
     * @param in_xmlFile Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processAlarmsXML(String in_xmlFile) {
        long fileId = 0;

        ConnectOracleDAO objConn = new ConnectOracleDAO(properties);
        Connection conn = null;

        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementAlarmsData = null;
        
        try {
            // Get connection - now throws Exception
            conn = objConn.getConnection();
            if (conn == null) {
                logger.error("Failed to get database connection");
                return -1;
            }

            preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            preparedStatementAlarmsData = conn.prepareStatement(INSERT_SQL_ALARMS);
            psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);

            // Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            // Retrieve Id generated for Detailed Table
            preparedStatementSelect.setString(1, in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();
            while (rs.next()) {
                fileId = rs.getLong(1);
            }
            rs.close();

            // Load XML file
            File file = new File(in_xmlFile);

            // Set parser to be namespace-aware
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            // Parse XML
            Document doc = dBuilder.parse(file);
            doc.getDocumentElement().normalize();

            // Namespace used in ITRON XML
            String ns = "http://www.itron.com/ItronInternalXsd/1.0/";

            // Get all events
            NodeList eventList = doc.getElementsByTagNameNS(ns, "Event");

            logger.info("Total alarms found: {}", eventList.getLength());
            int batchTotal = 0;
            
            for (int i = 0; i < eventList.getLength(); i++) {
                Element event = (Element) eventList.item(i);

                logger.debug("Alarm #{} - ObjectID: {}, EventType: {}", 
                    (i + 1), getText(event, ns, "ObjectID"), getText(event, ns, "EventType"));

                // Insert into ITRON_FILE_ALARMS
                preparedStatementAlarmsData.setLong(1, fileId);
                preparedStatementAlarmsData.setString(2, getText(event, ns, "CollectionSystemID"));
                preparedStatementAlarmsData.setString(3, getText(event, ns, "ObjectID"));
                preparedStatementAlarmsData.setString(4, getText(event, ns, "ObjectType"));
                preparedStatementAlarmsData.setString(5, getText(event, ns, "EventType"));
                preparedStatementAlarmsData.setString(6, getText(event, ns, "EventDateTime"));
                preparedStatementAlarmsData.setString(7, getText(event, ns, "CaptureDateTime"));
                preparedStatementAlarmsData.setString(8, getText(event, ns, "IsHistorical"));
                preparedStatementAlarmsData.addBatch();
                
                if (batchTotal++ == 4196) {
                    int[] resultData = preparedStatementAlarmsData.executeBatch();
                    preparedStatementAlarmsData.clearBatch();
                    batchTotal = 0;
                }
            }
            
            // Insert rest of records
            if (batchTotal > 0) {
                int[] resultData = preparedStatementAlarmsData.executeBatch();
            }
            
            // Update Process File
            psUpdProcess.setLong(1, 0L);
            psUpdProcess.setString(2, null);
            psUpdProcess.setLong(3, fileId);
            psUpdProcess.addBatch();
            int[] resultData = psUpdProcess.executeBatch();
            return 0;
            
        } catch (Exception e) {
            logger.error("Error processing alarms XML: {}", e.getMessage(), e);
            try {
                if (psUpdProcess != null && fileId > 0) {
                    psUpdProcess.setLong(1, -1L);
                    psUpdProcess.setString(2, e.getMessage());
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
            } catch (Exception ex) {
                logger.error("Failed to update process status", ex);
            }
            return -1;
        } finally {
            closeQuietly(preparedStatementAlarmsData);
            closeQuietly(preparedStatementSelect);
            closeQuietly(preparedStatementProcess);
            closeQuietly(psUpdProcess);
            closeQuietly(conn);
        }
    }

    // Helper: get element text
    private static String getText(Element parent, String ns, String tag) {
        NodeList nl = parent.getElementsByTagNameNS(ns, tag);
        return (nl != null && nl.getLength() > 0) ? nl.item(0).getTextContent() : "";
    }

    // Helper: get element object
    private static Element getElement(Element parent, String ns, String tag) {
        NodeList nl = parent.getElementsByTagNameNS(ns, tag);
        return (nl != null && nl.getLength() > 0) ? (Element) nl.item(0) : null;
    }

    /**
     * Process Readings XML file from ITRON SFTP
     * 
     * Steps:
     *   1. Validating XML Structure [ validateReadingsXML() ]
     *   2. If Specific Invalidation -> Fix File [ fixQuotes() ]
     *   3. Loading Valid XML Structure into Database
     * 
     * @param in_xmlFile Path to XML file
     * @return 0 on success, -1 on error
     */
    public static int processReadingsXML(String in_xmlFile) {
        String xmlFilePath = in_xmlFile;
        String xmlFilePathFixed = "READINGS_template_fixed.xml";
        String erroCoderMessg = null;
        String regex = "[#]";
        Integer errorRepair = 999;
        long startTime = System.currentTimeMillis();
        long totalDuration;
        long fileId = 0;

        ConnectOracleDAO objConn = new ConnectOracleDAO(properties);
        Connection conn = null;

        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementReadingsData = null;
        
        try {
            // Get connection - now throws Exception
            conn = objConn.getConnection();
            if (conn == null) {
                logger.error("Failed to get database connection");
                return -1;
            }

            preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
            preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
            preparedStatementReadingsData = conn.prepareStatement(INSERT_SQL_READINGS);
            psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);

            totalDuration = System.currentTimeMillis() - startTime;
            
            // Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            // Retrieve Id generated for Detailed Table
            preparedStatementSelect.setString(1, in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();
            while (rs.next()) {
                fileId = rs.getLong(1);
            }
            rs.close();

            // Path to your XML file
            erroCoderMessg = validateReadingsXML(xmlFilePath);
            logger.debug("erroCoderMessg: {}", erroCoderMessg);
            String[] erroCoderMessgArr = erroCoderMessg.split(regex);

            if (erroCoderMessgArr[0].contains("1")) {
                // Case where file can be fixed [ NO "" in both sides of attribute values ]
                errorRepair = fixQuotes(xmlFilePath, xmlFilePathFixed);
                logger.debug("errorRepair: {}", errorRepair);
                xmlFilePath = xmlFilePathFixed;
            }

            if ((erroCoderMessgArr[0].contains("0")) ||
                    (erroCoderMessgArr[0].contains("1") && errorRepair == 0)) {

                // Getting and Loading Data into DB
                File xmlFile = new File(xmlFilePath);

                // Create DocumentBuilder
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                dbFactory.setIgnoringComments(true);
                dbFactory.setIgnoringElementContentWhitespace(true);
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

                // Parse XML
                Document doc = dBuilder.parse(xmlFile);
                doc.getDocumentElement().normalize();

                logger.info("Root Element: {}", doc.getDocumentElement().getNodeName());

                // ====== HEADER ======
                Node headerNode = doc.getElementsByTagName("Header").item(0);
                if (headerNode != null) {
                    Element header = (Element) headerNode;
                    String systemId = header.getElementsByTagName("IEE_System")
                            .item(0).getAttributes().getNamedItem("Id").getTextContent();
                    String creationDatetime = header.getElementsByTagName("Creation_Datetime")
                            .item(0).getAttributes().getNamedItem("Datetime").getTextContent();
                    String timezone = header.getElementsByTagName("Timezone")
                            .item(0).getAttributes().getNamedItem("Id").getTextContent();
                    String filePath = header.getElementsByTagName("Path")
                            .item(0).getAttributes().getNamedItem("FilePath").getTextContent();
                    
                    logger.debug("Header - SystemID: {}, CreationDatetime: {}", systemId, creationDatetime);
                }

                // ====== CHANNELS ======
                NodeList channels = doc.getElementsByTagName("Channel");
                logger.info("Channels found: {}", channels.getLength());

                for (int i = 0; i < channels.getLength(); i++) {
                    Element channel = (Element) channels.item(i);
                    String servicePoint = channel
                            .getElementsByTagName("ChannelID")
                            .item(0).getAttributes().getNamedItem("ServicePointChannelID").getTextContent();
                    String startDate = channel.getAttribute("StartDate");
                    String endDate = channel.getAttribute("EndDate");
                    String intervalLength = channel.getAttribute("IntervalLength");
                    String isRegister = channel.getAttribute("IsRegister");

                    logger.debug("Channel {} - ServicePointChannelID: {}", (i + 1), servicePoint);

                    // Parsing tag that 1. immediately closes 2. is optional
                    NodeList nodeListTimePeriod = channel.getElementsByTagName("TimePeriod");
                    if (nodeListTimePeriod.getLength() != 0) {
                        String startRead = channel.getElementsByTagName("TimePeriod")
                                .item(0).getAttributes().getNamedItem("StartRead").getTextContent();
                        String endRead = channel.getElementsByTagName("TimePeriod")
                                .item(0).getAttributes().getNamedItem("EndRead").getTextContent();
                        logger.debug("TimePeriod - startRead: {}, endRead: {}", startRead, endRead);
                    }
                    
                    // ====== READINGS ======
                    NodeList readings = channel.getElementsByTagName("Reading");
                    int batchTotal = 0;

                    for (int j = 0; j < readings.getLength(); j++) {
                        Element reading = (Element) readings.item(j);
                        String value = reading.getAttribute("Value");
                        String statusRef = reading.getAttribute("StatusRef");
                        String readingTime = reading.hasAttribute("ReadingTime")
                                ? reading.getAttribute("ReadingTime")
                                : null;

                        logger.trace("Reading - Value: {}, StatusRef: {}, Time: {}", 
                            value, statusRef, readingTime);

                        // Insert into ITRON_FILE_READINGS
                        preparedStatementReadingsData.setLong(1, fileId);
                        preparedStatementReadingsData.setString(2, servicePoint);
                        preparedStatementReadingsData.setDouble(3, Double.parseDouble(value));
                        preparedStatementReadingsData.setString(4, statusRef);
                        preparedStatementReadingsData.setString(5, readingTime);
                        preparedStatementReadingsData.addBatch();
                        
                        if (batchTotal++ == 4196) {
                            int[] resultData = preparedStatementReadingsData.executeBatch();
                            preparedStatementReadingsData.clearBatch();
                            batchTotal = 0;
                        }
                    }
                    
                    // Insert rest of records
                    if (batchTotal > 0) {
                        int[] resultData = preparedStatementReadingsData.executeBatch();
                    }
                    
                    psUpdProcess.setLong(1, 0L);
                    psUpdProcess.setString(2, null);
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
                
                totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Readings processing completed - Duration: {}ms", totalDuration);
                return 0;
            } else {
                // XML validation failed and cannot be fixed
                try {
                    psUpdProcess.setLong(1, Long.parseLong(erroCoderMessgArr[0]));
                    psUpdProcess.setString(2, erroCoderMessgArr.length > 1 ? erroCoderMessgArr[1] : "Unknown error");
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                } catch (Exception ex) {
                    logger.error("Failed to update process status", ex);
                }
                return -1;
            }

        } catch (Exception e) {
            logger.error("Error processing readings XML: {}", e.getMessage(), e);
            try {
                if (psUpdProcess != null && fileId > 0) {
                    psUpdProcess.setLong(1, -1L);
                    psUpdProcess.setString(2, e.getMessage());
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
            } catch (Exception ex) {
                logger.error("Failed to update process status", ex);
            }
        } finally {
            closeQuietly(preparedStatementReadingsData);
            closeQuietly(preparedStatementSelect);
            closeQuietly(preparedStatementProcess);
            closeQuietly(psUpdProcess);
            closeQuietly(conn);
        }
        return -1;
    }

    /**
     * Validate XML structure
     * 
     * @param in_xmlFile Path to XML file
     * @return Error code and message separated by #
     */
    public static String validateReadingsXML(String in_xmlFile) {
        File xmlFile = new File(in_xmlFile);

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            factory.setNamespaceAware(true);
            factory.setValidating(false);
            factory.setIgnoringComments(true);
            factory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder builder = factory.newDocumentBuilder();

            // Attach a custom error handler
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
            
            // Parse and automatically check well-formedness
            Document doc = builder.parse(xmlFile);

        } catch (SAXParseException e) {
            if (e.getMessage().contains("Open quote is expected for attribute")) {
                return "1" + "#" + e.getMessage();
            }
            if (e.getMessage().contains("must not contain the")) {
                return "2" + "#" + e.getMessage();
            }
            if (e.getMessage().contains("must be terminated by the matching end-tag")) {
                return "3" + "#" + e.getMessage();
            }
            if (e.getMessage().contains("must be followed by either attribute specifications")) {
                return "4" + "#" + e.getMessage();
            } else {
                return "5" + "#" + e.getMessage();
            }

        } catch (Exception e) {
            logger.error("XML validation error", e);
            return "-1" + "#" + e.getMessage();
        }

        return "0" + "#";
    }

    /**
     * Fix unquoted attributes in XML
     * 
     * @param in_xmlFile Input XML file path
     * @param in_xmlFile_fixed Output fixed XML file path
     * @return 1 on success, -1 on error
     */
    public static Integer fixQuotes(String in_xmlFile, String in_xmlFile_fixed) {
        logger.debug("Fixing quotes - Input: {}", in_xmlFile);
        Path input = Paths.get(in_xmlFile);
        Path fixed = Paths.get(in_xmlFile_fixed);

        try {
            // Java 8 compatible file reading
            File file = new File(in_xmlFile);
            StringBuilder content = new StringBuilder();
            Scanner scanner = new Scanner(file, "UTF-8");
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
            String fixedText = sb.toString();

            // Java 8 compatible file writing
            PrintWriter writer = new PrintWriter(in_xmlFile_fixed, "UTF-8");
            writer.print(fixedText);

            scanner.close();
            writer.close();

            logger.info("Repaired XML written to: {}", fixed.toAbsolutePath());

            // Parsing the repaired file to confirm validity
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(fixed.toFile());
            logger.info("XML parsed successfully. Root element: {}", 
                doc.getDocumentElement().getNodeName());
            return 1;
        } catch (Exception e) {
            logger.error("Error fixing quotes", e);
            return -1;
        }
    }

    /**
     * Quietly close a PreparedStatement
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
     * Quietly close a Connection
     */
    private static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Error closing connection", e);
            }
        }
    }
}
