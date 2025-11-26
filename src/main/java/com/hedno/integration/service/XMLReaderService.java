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
 * Reading XML data from SFTP SErver.
 * This is used when we need to parse XML data to store into the database.
 *
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class XMLReaderService {
    private static final Logger logger = LoggerFactory.getLogger(XMLReaderService.class);
    private static final Properties properties = new Properties();

    static {
        try ( InputStream input = XMLReaderService.class.getClassLoader().getResourceAsStream("config/application.properties") ) {

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
    //private static long fileId = 0;
    public static String get(String key) {
        return properties.getProperty(key);
    }

    //public static ConnectOracleDAO objConn = new ConnectOracleDAO(properties);

    //public static PreparedStatement preparedStatementSelect = objConn.getPreparedStatement(SELECT_SQL_PROCESS);
    //public static PreparedStatement preparedStatementProcess = objConn.getPreparedStatement(INSERT_SQL_PROCESS);
    //public static PreparedStatement preparedStatementEventsData = objConn.getPreparedStatement(INSERT_SQL_EVENTS);
    //public static PreparedStatement preparedStatementReadingsData = objConn.getPreparedStatement(INSERT_SQL_READINGS);
    //public static PreparedStatement psUpdProcess = objConn.getPreparedStatement(UPDATE_SQL_PROCESS);

    public static int processEventsXML(String in_xmlFile) {
 
    	ConnectOracleDAO objConn = new ConnectOracleDAO(properties);
    	Connection conn = objConn.getConnection();
    
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
	    	preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
	    	preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
	    	preparedStatementEventsData = conn.prepareStatement(INSERT_SQL_EVENTS);
	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);
    	
            //Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            //Retrieve Id generated for Detailed Table
            //preparedStatementSelect = objConn.getPreparedStatement(INSERT_SQL_PROCESS);
            preparedStatementSelect.setString(1, in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();

            while (rs.next()) {
                fileId = rs.getLong(1);
            }
            rs.close();
            //preparedStatementSelect.close();

            // 1. Create a DocumentBuilderFactory
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // For this simple case, we can ignore namespaces
            factory.setNamespaceAware(false);

            // 2. Build the document
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            System.out.println("Root element: " + doc.getDocumentElement().getNodeName());

            // 3. Get all <Event> elements
            NodeList eventNodes = doc.getElementsByTagName("Event");
            System.out.println("Total events: " + eventNodes.getLength());

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

                    System.out.println("\nEvent #" + (i + 1));
                    System.out.println("  CollectionSystemID: " + collectionSystemId);
                    System.out.println("  ObjectID:           " + objectId);
                    System.out.println("  ObjectType:         " + objectType);
                    System.out.println("  EventType:          " + eventType);
                    System.out.println("  EventDateTime:      " + eventDateTime);
                    System.out.println("  CaptureDateTime:    " + captureDateTime);

                    //Insert into ITRON_FILE_EVENTS
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
            //Insert rest of records
            if (batchTotal > 0) {
                int[] resultData = preparedStatementEventsData.executeBatch();
            }
            psUpdProcess.setLong(1, 0L);
            psUpdProcess.setString(2, null);
            psUpdProcess.setLong(3, fileId);
            psUpdProcess.addBatch();
            int[] resultData = psUpdProcess.executeBatch();
            return 0;

        } /*catch (SAXParseException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return -1;
        } */
        catch (Exception e) {
            logger.info("for update fileId :" + fileId);
            logger.error(e.getMessage());
            e.printStackTrace();

            try{
                psUpdProcess.setLong(1, -1L);
                psUpdProcess.setString(2, e.getMessage());
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
            }
            catch (Exception ex){
                e.printStackTrace();
            }
            return -1;
        } finally {
        	try {
				conn.close();
			} catch (SQLException e) {
	            logger.error("error while closing connection :", e);
				e.printStackTrace();
			}
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
     * 
     * @param in_xmlFile
     * @return
     */
    public static int processAlarmsXML(String in_xmlFile) {
        long fileId = 0;

    	ConnectOracleDAO objConn = new ConnectOracleDAO(properties);
    	Connection conn = objConn.getConnection();
    	
        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementAlarmsData = null;
        try {
	    	preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
	    	preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
	    	preparedStatementAlarmsData = conn.prepareStatement(INSERT_SQL_ALARMS);
	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);        



            //Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            //Retrieve Id generated for Detailed Table
            preparedStatementSelect.setString(1,in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();
            while (rs.next() ) {
                fileId = rs.getLong(1);
            }
            rs.close();
            //preparedStatementSelect.close();

            // Load XML file
            File file = new File(in_xmlFile);

            // Set parser to be namespace-aware
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            dbFactory.setNamespaceAware(true);
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

            // Parse XML
            Document doc = dBuilder.parse(file);
            doc.getDocumentElement().normalize();

            // Namespace used in your XML
            String ns = "http://www.itron.com/ItronInternalXsd/1.0/";

            // Get all events
            NodeList eventList = doc.getElementsByTagNameNS(ns, "Event");

            System.out.println("Total events found: " + eventList.getLength());
            int batchTotal = 0;
            for (int i = 0; i < eventList.getLength(); i++) {
                Element event = (Element) eventList.item(i);

                System.out.println("\n===== EVENT " + (i+1) + " =====");
                System.out.println("CollectionSystemID: " + getText(event, ns, "CollectionSystemID"));
                System.out.println("ObjectID: " + getText(event, ns, "ObjectID"));
                System.out.println("ObjectType: " + getText(event, ns, "ObjectType"));
                System.out.println("EventType: " + getText(event, ns, "EventType"));
                System.out.println("IsHistorical: " + getText(event, ns, "IsHistorical"));
                System.out.println("EventDateTime: " + getText(event, ns, "EventDateTime"));
                System.out.println("CaptureDateTime: " + getText(event, ns, "CaptureDateTime"));

                // Extract EventData → Data → Name/Value
                Element eventData = getElement(event, ns, "EventData");
                if (eventData != null) {
                    Element data = getElement(eventData, ns, "Data");
                    if (data != null) {
                        System.out.println("EventData.Name: " + getText(data, ns, "Name"));
                        System.out.println("EventData.Value: " + getText(data, ns, "Value"));
                    }
                }
                System.out.println("AccountNumber: " + getText(event, ns, "AccountNumber"));
                System.out.println("CollectorID: " + getText(event, ns, "CollectorID"));
                System.out.println("MeterID: " + getText(event, ns, "MeterID"));
                System.out.println("MeterNumber: " + getText(event, ns, "MeterNumber"));
                System.out.println("TransformerID: " + getText(event, ns, "TransformerID"));

                //Insert into ITRON_FILE_ALARMS
                preparedStatementAlarmsData.setLong(1, fileId);
                preparedStatementAlarmsData.setString(2, getText(event, ns, "CollectionSystemID"));
                preparedStatementAlarmsData.setString(3, getText(event, ns, "ObjectID"));
                preparedStatementAlarmsData.setString(4, getText(event, ns, "ObjectType"));
                preparedStatementAlarmsData.setString(5, getText(event, ns, "EventType"));
                preparedStatementAlarmsData.setString(6, getText(event, ns, "EventDateTime"));
                preparedStatementAlarmsData.setString(7,  getText(event, ns, "CaptureDateTime"));
                preparedStatementAlarmsData.setString(8,  getText(event, ns, "IsHistorical"));
                preparedStatementAlarmsData.addBatch();
                if (batchTotal++ == 4196) {
                    int[] resultData = preparedStatementAlarmsData.executeBatch();
                    preparedStatementAlarmsData.clearBatch();
                    batchTotal = 0;
                }
            }
            //Insert rest of records
            if (batchTotal > 0) {
                int[] resultData = preparedStatementAlarmsData.executeBatch();
            }
            //Update Process File
            psUpdProcess.setLong(1, 0L);
            psUpdProcess.setString(2, null);
            psUpdProcess.setLong(3, fileId);
            psUpdProcess.addBatch();
            int[] resultData = psUpdProcess.executeBatch();
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            try{
                psUpdProcess.setLong(1, -1L);
                psUpdProcess.setString(2, e.getMessage());
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
            }
            catch (Exception ex){
                e.printStackTrace();
            }
            return -1;
        } finally {
         	try {
				conn.close();
			} catch (SQLException e) {
	            logger.error("error while closing connection :", e);
				e.printStackTrace();
			}
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


    /*
    processReadingsXML(String in_xmlFile)
    Steps
            1. Validating XML Structure [ validateReadingsXML() ]
            2. If Specific Invalidation -> Fix  File [ fixQuotes() ]
            3. Loading Valid XML Structure into Database
     */
    /**
     * 
     * @param in_xmlFile
     * @return
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
    	Connection conn = objConn.getConnection();
    	
        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementSelect = null;
        PreparedStatement preparedStatementReadingsData = null;
        try {
	    	preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS);
	    	preparedStatementSelect = conn.prepareStatement(SELECT_SQL_PROCESS);
	    	preparedStatementReadingsData = conn.prepareStatement(INSERT_SQL_READINGS);
	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);
	    	
            totalDuration = System.currentTimeMillis() - startTime;
            //Insert into ITRON_FILE_PROCESS
            preparedStatementProcess.setString(1, in_xmlFile);
            preparedStatementProcess.addBatch();
            int[] resultProcess = preparedStatementProcess.executeBatch();

            //Retrieve Id generated for Detailed Table
            preparedStatementSelect.setString(1,in_xmlFile);
            ResultSet rs = preparedStatementSelect.executeQuery();
            while (rs.next() ) {
                fileId = rs.getLong(1);
            }
            rs.close();
            //preparedStatementSelect.close();


            // Path to your XML file
            erroCoderMessg = validateReadingsXML(xmlFilePath);
            //System.out.format("errorProcessed :%s errorMsg :%s\n", errorProcessed , out_errorMsg);
            //System.out.printf("errorProcessed :%s errorMsg :%s\n", errorProcessed , out_errorMsg);
            System.out.printf("erroCoderMessg :%s\n", erroCoderMessg);
            String[] erroCoderMessgArr = erroCoderMessg.split(regex);
            //System.out.println("[0] =" + erroCoderMessgArr[0] + "\n");

            //if ( !erroCoderMessgArr[0].contains("1") ) return -2;//invalid File can not be fixed

            if (erroCoderMessgArr[0].contains("1")) {
                //case where file can be fixed [ NO "" in both sides of attribute values ]

                //System.out.println("xmlFilePath :" + xmlFilePath );
                //System.out.println("xmlFilePathFixed :" + xmlFilePathFixed );
                errorRepair = fixQuotes(xmlFilePath, xmlFilePathFixed);
                System.out.println("errorRepair :" + errorRepair);
                xmlFilePath = xmlFilePathFixed;
            }

            if ((erroCoderMessgArr[0].contains("0")) ||
                    (erroCoderMessgArr[0].contains("1") && errorRepair == 0)
            ) {

                //Getting and Loading Data into DB
                File xmlFile = new File(xmlFilePath);

                // Create DocumentBuilder
                //XML parsers like DocumentBuilder cannot parse broken XML
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                dbFactory.setIgnoringComments(true);
                dbFactory.setIgnoringElementContentWhitespace(true);
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

                // Parse XML
                Document doc = dBuilder.parse(xmlFile);
                doc.getDocumentElement().normalize();

                System.out.println("Root Element: " + doc.getDocumentElement().getNodeName());

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
                    /*
                    System.out.println("\nHEADER INFO:");
                    System.out.println("System ID: " + systemId);
                    System.out.println("Creation Datetime: " + creationDatetime);
                    System.out.println("Timezone: " + timezone);
                    System.out.println("File Path: " + filePath);
                    */
                }

                // ====== CHANNELS ======
                NodeList channels = doc.getElementsByTagName("Channel");
                System.out.println("\nCHANNELS FOUND: " + channels.getLength());

                for (int i = 0; i < channels.getLength(); i++) {
                    Element channel = (Element) channels.item(i);
                    String servicePoint = channel
                            .getElementsByTagName("ChannelID")
                            .item(0).getAttributes().getNamedItem("ServicePointChannelID").getTextContent();
                    String startDate = channel.getAttribute("StartDate");
                    String endDate = channel.getAttribute("EndDate");
                    String intervalLength = channel.getAttribute("IntervalLength");
                    String isRegister = channel.getAttribute("IsRegister");

                    System.out.println("\nChannel " + (i + 1) + ":");
                    System.out.println("ServicePointChannelID: " + servicePoint);
                    System.out.println("StartDate: " + startDate);
                    System.out.println("EndDate: " + endDate);
                    System.out.println("IntervalLength: " + intervalLength);
                    System.out.println("IsRegister: " + isRegister);


                    // Parsing tag that 1. immediatelly closes 2. is optional
                    NodeList nodeListTimePeriod = channel.getElementsByTagName("TimePeriod");
                    if (nodeListTimePeriod.getLength() != 0) {
                        String startRead = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("StartRead").getTextContent();
                        String endRead = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("EndRead").getTextContent();
                        System.out.println("\nstartRead :" + startRead + " endRead :" + endRead + "\n");
                    }
                    // ====== READINGS ======
                    NodeList readings = channel.getElementsByTagName("Reading");
                    //System.out.println("  Total Readings: " + readings.getLength());
                    int batchTotal = 0;

                    for (int j = 0; j < readings.getLength(); j++) {
                        Element reading = (Element) readings.item(j);
                        String value = reading.getAttribute("Value");
                        String statusRef = reading.getAttribute("StatusRef");
                        String readingTime = reading.hasAttribute("ReadingTime")
                                ? reading.getAttribute("ReadingTime")
                                :null;

                        System.out.println("    Value=" + value + ", StatusRef=" + statusRef + ", Time=" + readingTime);
                        /*
                        Static member 'com.hedno.integration.dao.ConnectOracle.getPreparedStatement(java.lang.String)' accessed via instance reference
                        Inspection info: Reports references to static methods and fields via a class instance rather than the class itself.
                        Even though referring to static members via instance variables is allowed by The Java Language Specification,
                        this makes the code confusing as the reader may think that the result of the method depends on the instance.
                        The quick-fix replaces the instance variable with the class name.
                         */
                        //Insert into ITRON_FILE_READINGS
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
                    //Insert rest of records
                    if (batchTotal > 0) {
                        int[] resultData = preparedStatementReadingsData.executeBatch();
                    }
                    psUpdProcess.setLong(1, 0L);
                    psUpdProcess.setString(2, null);
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                    //conn.commit();
                }
                totalDuration = System.currentTimeMillis() - startTime;
                System.out.println("Duration :" + (totalDuration / 1000) / 60 + " Mins");
                System.out.println("Duration :" + (totalDuration / 1000) + " Secs");
                System.out.println("Duration :" + totalDuration + " Millisecs");
                return 0;
            }
            else{
                try{
                    psUpdProcess.setLong(1, Long.parseLong(erroCoderMessgArr[0]));
                    psUpdProcess.setString(2, erroCoderMessgArr[1]);
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
                catch (Exception ex){
                    ex.printStackTrace();
                }
                finally{
                    //psUpdProcess = null;
                }
                return -1;
            }

        }
        catch (Exception e) {
            e.printStackTrace();
            try{
                psUpdProcess.setLong(1, -1L);
                psUpdProcess.setString(2, e.getMessage());
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
            }
            catch (Exception ex){
                e.printStackTrace();
            }
        }
        finally {
         	try {
				conn.close();
			} catch (SQLException e) {
	            logger.error("error while closing connection :", e);
				e.printStackTrace();
			}
        }
        return -1;
    }

    public static String validateReadingsXML(String in_xmlFile) {
        File xmlFile = new File(in_xmlFile);//file_template_fixed

        try{
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            factory.setNamespaceAware(true);    // Handle xmlns properly
            factory.setValidating(false);       // Only check well-formedness
            factory.setIgnoringComments(true);
            factory.setIgnoringElementContentWhitespace(true);
            DocumentBuilder builder = factory.newDocumentBuilder();

            // Attach a custom error handler
            builder.setErrorHandler(new ErrorHandler() {
                                        @Override
                                        public void warning(SAXParseException e) {
                                            System.err.println("Warning: " + e.getMessage());
                                        }
                                        @Override
                                        public void error(SAXParseException e) throws SAXException {
                                            System.err.println("Error: " + e.getMessage());
                                            throw e; // stop parsing
                                        }
                                        @Override
                                        public void fatalError(SAXParseException e) throws SAXException {
                                            System.err.println("Fatal: " + e.getMessage());
                                            throw e; // stop parsing
                                        }
                                    }
            );
            // Parse and automatically check well-formedness
            Document doc = builder.parse(xmlFile);
            //System.out.println("XML is well-formed! Root element: " + doc.getDocumentElement().getNodeName()  + "\n");

        }catch (SAXParseException e) {
            /*
            System.err.println("\nXML NOT WELL-FORMED");
            System.err.println(" Line " + e.getLineNumber() + ", Column " + e.getColumnNumber());
            System.err.println("  " + e.getMessage() );
            */
            if ( e.getMessage().contains("Open quote is expected for attribute") ) return "1" + "#" + e.getMessage();
            if ( e.getMessage().contains("must not contain the") ) return "2" + "#" + e.getMessage();
            if ( e.getMessage().contains("must be terminated by the matching end-tag") ) return "3" + "#" + e.getMessage();

            if ( e.getMessage().contains("must be followed by either attribute specifications") ) {
                return "4" + "#" + e.getMessage();
            }
            else{
                return "5" + "#" + e.getMessage();
            }

        }catch (Exception e) {
            e.printStackTrace();
            return "-1"+ "#" + e.getMessage();
        }
        finally{
            xmlFile = null;
        }

        return "0" + "#";
    }

    public static Integer fixQuotes(String in_xmlFile,String in_xmlFile_fixed){
        System.out.println("in_xmlFile:" + in_xmlFile);
        Path input = Paths.get(in_xmlFile);
        Path fixed = Paths.get(in_xmlFile_fixed);

        try {
            // Read file content as text from Java 11
            //String xmlText = Files.readString(input);

            //Java 8
            File file = new File(in_xmlFile);
            StringBuilder content = new StringBuilder();
            Scanner scanner = new Scanner(file,"UTF-8");
            while (scanner.hasNextLine()) {
                content.append(scanner.nextLine()).append(System.lineSeparator());
            }

            String xmlText = content.toString();
            //System.out.println("\nxmlText :\n" + xmlText );
            //Add quotes around unquoted attribute values
            Matcher m = ATTR_NO_QUOTES.matcher(xmlText);
            StringBuffer sb = new StringBuffer();
            System.out.println("entered");
            while (m.find()) {
                String attr = m.group(1);
                String val  = m.group(2);
                m.appendReplacement(sb, attr + "=\"" + val + "\"");
            }
            m.appendTail(sb);
            String fixedText = sb.toString();

            //  Save the repaired XML
            // Write file content as text from Java 11
            //Files.writeString(fixed, fixedText);
            //Java 8
            // Open output file for writing
            PrintWriter writer = new PrintWriter(in_xmlFile_fixed, "UTF-8");
            writer.print(fixedText);

            scanner.close();
            writer.close();

            System.out.println("Repaired XML written to: " + fixed.toAbsolutePath());

            // parsing the repaired file to confirm validity
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(fixed.toFile());
            System.out.println("XML parsed successfully. Root element: " + doc.getDocumentElement().getNodeName());
            return 1;
        }
        catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }
}
