package com.hedno.integration.service;

import com.hedno.integration.dao.ConnectOracleDAO;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintWriter;
import org.w3c.dom.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//import com.hedno.integration.dao.ChannelDAO;
//import com.hedno.integration.dao.ReadingDAO;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Reading XML data that were gotten from SFTP SEVER.
 * This is used when we need to parse XML data to store into the database.
 *
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class XMLReaderService {
    private static final Logger logger = LoggerFactory.getLogger(XMLReaderService.class);
    private static final Properties properties = new Properties();
    // Define formatter for desired output
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private static final Pattern ATTR_NO_QUOTES = Pattern.compile("(\\s+[\\w:]+)=([^\"'\\s>]+)");
    //MASTER TABLE [ SMC_MDM_SCCURVES_HD ]
    //private static final String SELECT_SQL_PROCESS = "SELECT max(LOG_ID) FROM SMC_MDM_SCCURVES_HD WHERE FILE_NAME = ?";
    private static final String INSERT_SQL_PROCESS = "INSERT INTO SMC_MDM_SCCURVES_HD ( SOURCE_SYSTEM, SOURCE_TYPE, FILE_NAME) values ('ITRON', ?, ?)";
    private static final String UPDATE_SQL_PROCESS = "UPDATE SMC_MDM_SCCURVES_HD SET STATUS = ?, STATUS_MSG = ? WHERE LOG_ID = ?";
    //DETAILS TABLES
    private static final String DELETE_SQL_CURVES_READINGS =  "DELETE FROM SMC_MDM_SCCURVES where HD_LOG_ID = ?";
    private static final String INSERT_SQL_ALARMS = "INSERT INTO SMC_ALARMS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME, IS_HISTORICAL) values (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_EVENTS = "INSERT INTO SMC_EVENTS (F_ID, COLLECTION_SYSTEM_ID, OBJECT_ID, OBJECT_TYPE, EVENT_TYPE, EVENT_DTIME, CAPTURE_DTIME) values (?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_STATUS = "INSERT INTO SMC_MDM_STATUS_DT (HD_LOG_ID, STATUS_REF, SOURCE_VALID, STATUS_CODE, VALUE_FLG) values (?, ?, ?, ?, ?)";

    private static long fileLogId;

    public static String get(String key) {
        return properties.getProperty(key);
    }

    //public static ConnectOracleDAO objConn = new ConnectOracleDAO(properties);

    //public static PreparedStatement preparedStatementSelect = objConn.getPreparedStatement(SELECT_SQL_PROCESS);
    //public static PreparedStatement preparedStatementProcess = objConn.getPreparedStatement(INSERT_SQL_PROCESS);
    //public static PreparedStatement preparedStatementEventsData = objConn.getPreparedStatement(INSERT_SQL_EVENTS);
    //public static PreparedStatement preparedStatementReadingsData = objConn.getPreparedStatement(INSERT_SQL_READINGS);
    //public static PreparedStatement psUpdProcess = objConn.getPreparedStatement(UPDATE_SQL_PROCESS);

    /**
     *
     * @param dateTimeZone  2025-09-28T01:00:00+03:00    2025-09-28 T 01:00:00   +03:00
     * @return String
     */
    public static String formatToDate(String dateTimeZone) {
        return "to_date('" + dateTimeZone.substring(0, 10)  +  "','yyyy-mm-dd')";
    }

    /**
     *
     * @param num
     * @return String
     */
    public static String twoDigits(int num){
        return (num <= 9) ? "0" + num : String.valueOf(num);
    }

    /*

     */
    public static int assignTimeToCurve(String time){
        //input as hh:mm:ss -> 01:00:00
        String hour = time.substring(0, 2);
        //String minute = time.substring(3, 5);
        //String second = time.substring(6, 8);
        String minuteSecond = time.substring(3, 5) + ":" + time.substring(6, 8);
        //logger.info("hour ="+ hour + " minuteSecond =" + minuteSecond);
        int retCurve = 0;

        //check pattern mm:ss
        if ( minuteSecond.compareTo("00:00") >= 0  && minuteSecond.compareTo("14:59")  <= 0){
            retCurve = ( Integer.parseInt(hour) *  4 ) + 1;
        } else if ( minuteSecond.compareTo("15:00") >= 0  && minuteSecond.compareTo("29:59")  <= 0){
            retCurve =( Integer.parseInt(hour) *  4 ) + 2;
        } else if ( minuteSecond.compareTo("30:00") >= 0 && minuteSecond.compareTo("44:59")  <= 0){
            retCurve = ( Integer.parseInt(hour) *  4 ) + 3;
        } else if ( minuteSecond.compareTo("45:00") >= 0  && minuteSecond.compareTo("59:59")  <= 0){
            retCurve = ( Integer.parseInt(hour) *  4 ) + 4;;
        }
        return retCurve;
    }

    public static long insertHeader(Connection conn,PreparedStatement preparedStatementProcess, String fileName, String sourceType) throws Exception{
        long retId = -1;
        preparedStatementProcess.setString(1, sourceType);
        preparedStatementProcess.setString(2, fileName);

        //preparedStatementProcess.addBatch();
        int rows = preparedStatementProcess.executeUpdate();
        if (rows > 0) {
            // Get generated key
            ResultSet rs = preparedStatementProcess.getGeneratedKeys();
            if (rs.next()) {
                retId = rs.getLong(1);
            }
        }
        //int[] resultProcess = preparedStatementProcess.executeBatch();
        //preparedStatementProcess.clearBatch();
        return retId;
    }
    public static int processEventsXML(String in_xmlFile) {
    	ConnectOracleDAO objConn = new ConnectOracleDAO();
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
        PreparedStatement preparedStatementEventsData = null;
        try {
        	conn = objConn.getConnection();
            preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS,new String[] { "LOG_ID" });
	    	preparedStatementEventsData = conn.prepareStatement(INSERT_SQL_EVENTS);
	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);

            //Insert into SMC_MDM_DATA_HD
            fileId = insertHeader(conn,preparedStatementProcess,in_xmlFile,"EVENT");
            fileLogId = fileId;

            // 1. Create a DocumentBuilderFactory
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // For this simple case, we can ignore namespaces
            factory.setNamespaceAware(false);

            // 2. Build the document
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            logger.info("Root element: " + doc.getDocumentElement().getNodeName());

            // 3. Get all <Event> elements
            NodeList eventNodes = doc.getElementsByTagName("Event");
            logger.info("Total events: " + eventNodes.getLength());

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

                    logger.info("\nEvent #" + (i + 1));
                    logger.info("  CollectionSystemID: " + collectionSystemId);
                    logger.info("  ObjectID:           " + objectId);
                    logger.info("  ObjectType:         " + objectType);
                    logger.info("  EventType:          " + eventType);
                    logger.info("  EventDateTime:      " + eventDateTime);
                    logger.info("  CaptureDateTime:    " + captureDateTime);

                    //Insert into SCM_EVENTS
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
            //Update HEADER
            psUpdProcess.setString(1, "SUCCESS");
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
                psUpdProcess.setString(1, "ERROR");
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
			} catch (Exception e) {
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
     * @param in_xmlFile
     * @return
     *///Insert into SMC_MDM_DATA_HD
    public static int processAlarmsXML(String in_xmlFile) {
        long fileId = 0;

    	ConnectOracleDAO objConn = new ConnectOracleDAO();
    	Connection conn = null;
    	
        PreparedStatement psUpdProcess = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementAlarmsData = null;
        try {
        	conn = objConn.getConnection();
            preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS,new String[] { "LOG_ID" });
	    	preparedStatementAlarmsData = conn.prepareStatement(INSERT_SQL_ALARMS);
	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);

            //Insert into SMC_MDM_DATA_HD
            fileId = insertHeader(conn,preparedStatementProcess,in_xmlFile,"ALARM");
            fileLogId = fileId;

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

            logger.info("Total events found: " + eventList.getLength());
            int batchTotal = 0;
            for (int i = 0; i < eventList.getLength(); i++) {
                Element event = (Element) eventList.item(i);

                logger.info("\n===== EVENT " + (i+1) + " =====");
                logger.info("CollectionSystemID: " + getText(event, ns, "CollectionSystemID"));
                logger.info("ObjectID: " + getText(event, ns, "ObjectID"));
                logger.info("ObjectType: " + getText(event, ns, "ObjectType"));
                logger.info("EventType: " + getText(event, ns, "EventType"));
                logger.info("IsHistorical: " + getText(event, ns, "IsHistorical"));
                logger.info("EventDateTime: " + getText(event, ns, "EventDateTime"));
                logger.info("CaptureDateTime: " + getText(event, ns, "CaptureDateTime"));

                // Extract EventData → Data → Name/Value
                Element eventData = getElement(event, ns, "EventData");
                if (eventData != null) {
                    Element data = getElement(eventData, ns, "Data");
                    if (data != null) {
                        logger.info("EventData.Name: " + getText(data, ns, "Name"));
                        logger.info("EventData.Value: " + getText(data, ns, "Value"));
                    }
                }
                logger.info("AccountNumber: " + getText(event, ns, "AccountNumber"));
                logger.info("CollectorID: " + getText(event, ns, "CollectorID"));
                logger.info("MeterID: " + getText(event, ns, "MeterID"));
                logger.info("MeterNumber: " + getText(event, ns, "MeterNumber"));
                logger.info("TransformerID: " + getText(event, ns, "TransformerID"));

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
            psUpdProcess.setString(1, "SUCCESS");
            psUpdProcess.setString(2, null);
            psUpdProcess.setLong(3, fileId);
            psUpdProcess.addBatch();
            int[] resultData = psUpdProcess.executeBatch();
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            try{
                psUpdProcess.setString(1, "ERROR");
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
			} catch (Exception e) {
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
     * @return int <br>
        0:success<br>
        1:Open quote is expected for attribute<br>
        2:must not contain the<br>
        3:must be terminated by the matching end-tag<br>
        4:must be followed by either attribute specifications<br>
        5:other<br>
        -1:System error<br>
     @throws
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
        String curvesTimeCalc = null;
        String curvesDateCalc = null;
        Set<Integer> setInvalidInterval= new HashSet<Integer>();
   
    	ConnectOracleDAO objConn = new ConnectOracleDAO();
    	Connection conn = null;
    	
        PreparedStatement psUpdProcess = null;
        PreparedStatement psDelCurvesReadings = null;
        PreparedStatement preparedStatementProcess = null;
        PreparedStatement preparedStatementReadingsData = null;
        //PreparedStatement psCurvesReadingsData = null;
        PreparedStatement psCurvesReadingsDataRow = null;
        PreparedStatement psRegistersReadingsData = null;
        PreparedStatement psStatusInsertingData = null;
        try {
        	conn = objConn.getConnection();
	    	preparedStatementProcess = conn.prepareStatement(INSERT_SQL_PROCESS,new String[] { "LOG_ID" });
	    	//preparedStatementReadingsData = conn.prepareStatement(INSERT_SQL_READINGS);
            //psCurvesReadingsData = conn.prepareStatement(INSERT_SQL_CURVES_READINGS);

	    	psUpdProcess = conn.prepareStatement(UPDATE_SQL_PROCESS);
            psDelCurvesReadings = conn.prepareStatement(DELETE_SQL_CURVES_READINGS);
            psStatusInsertingData = conn.prepareStatement(INSERT_SQL_STATUS);
	    	
            totalDuration = System.currentTimeMillis() - startTime;
            //Insert into HEADER table SMC_MDM_SCCURVES_HD
            fileId = insertHeader(conn,preparedStatementProcess,in_xmlFile,"MEASURE");
            fileLogId = fileId;

            // Path to your XML file
            erroCoderMessg = validateReadingsXML(xmlFilePath);
            logger.info("erroCoderMessg :" + erroCoderMessg);
            String[] erroCoderMessgArr = erroCoderMessg.split(regex);
            //logger.info("[0] =" + erroCoderMessgArr[0] + "\n");

            //if ( !erroCoderMessgArr[0].contains("1") ) return -2;//invalid File can not be fixed

            if (erroCoderMessgArr[0].contains("1")) {
                //case where file can be fixed [ NO "" in both sides of attribute values ]

                //logger.info("xmlFilePath :" + xmlFilePath );
                //logger.info("xmlFilePathFixed :" + xmlFilePathFixed );
                errorRepair = fixQuotes(xmlFilePath, xmlFilePathFixed);
                logger.info("errorRepair :" + errorRepair);
                xmlFilePath = xmlFilePathFixed;
            }

            //Check if (Valid File) OR (Was Not Valid but now fixed) Then process and store in DB
            if ( (erroCoderMessgArr[0].contains("0")) || (erroCoderMessgArr[0].contains("1") && errorRepair == 0) ) {

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

                logger.info("Root Element: " + doc.getDocumentElement().getNodeName());

                // ===================== tag <ReadingStatusRefTable> ==========================
                NodeList lstStatus = doc.getElementsByTagName("ReadingStatusRef");
                logger.info("ReadingStatusRef FOUND: " + lstStatus.getLength());

                for (int i = 0; i < lstStatus.getLength(); i++) {
                    Element refElement = (Element) lstStatus.item(i);

                    // Attribute Ref
                    String refValue = refElement.getAttribute("Ref");
                    System.out.println("Ref = " + refValue);

                    // UnencodedStatus
                    Element unencoded = (Element)
                            refElement.getElementsByTagName("UnencodedStatus").item(0);

                    String sourceValidation = unencoded.getAttribute("SourceValidation");
                    System.out.println("SourceValidation = " + sourceValidation);

                    // Status codes
                    NodeList codes = unencoded.getElementsByTagName("Code");

                    logger.info("Status Codes:");
                    String statusPttrn = "VEESET1";
                    for (int j = 0; j < codes.getLength(); j++) {
                        logger.info("  - " + codes.item(j).getTextContent());
                        String statusCode = codes.item(j).getTextContent();

                        psStatusInsertingData.setLong(1, fileId);
                        psStatusInsertingData.setString(2, refValue );
                        psStatusInsertingData.setString(3,  sourceValidation );
                        psStatusInsertingData.setString(4,statusCode );

                        int beginIndex = statusCode.length() - 2;
                        //ONLY when code = VEESET1xx( may also exist VEESET2xx , VEESET3xx ) get part xx
                        String valueFlg = ( statusCode.indexOf(statusPttrn) != -1 ) ? statusCode.substring(beginIndex) : "";
                        if ( statusCode.equals("ESTIMATED") ){ valueFlg = "ES";}
                        psStatusInsertingData.setString(5,valueFlg );
                        psStatusInsertingData.addBatch();
                        int[] resulStatusIns = psStatusInsertingData.executeBatch();
                    }
                }

                String creationDatetimeHDR = "";
                // ====== tag <HEADER> ======
                Node headerNode = doc.getElementsByTagName("Header").item(0);
                if (headerNode != null) {
                    Element header = (Element) headerNode;

                    String systemId = header.getElementsByTagName("IEE_System")
                            .item(0).getAttributes().getNamedItem("Id").getTextContent();

                    creationDatetimeHDR = header.getElementsByTagName("Creation_Datetime").item(0).getAttributes().getNamedItem("Datetime").getTextContent().substring(0,10);

                    String timezoneHeader = header.getElementsByTagName("Timezone")
                            .item(0).getAttributes().getNamedItem("Id").getTextContent();
                    String filePath = header.getElementsByTagName("Path")
                            .item(0).getAttributes().getNamedItem("FilePath").getTextContent();
                    /*
                    logger.info("\nHEADER INFO:");
                    logger.info("System ID: " + systemId);
                    logger.info("Creation Datetime: " + creationDatetime);
                    logger.info("Timezone: " + timezone);
                    logger.info("File Path: " + filePath);
                    */
                }
                StringBuilder blrCurvesColumnsHd = null;
                StringBuilder blrCurvesValuesHd = null;
                StringBuilder blrRegistersColumnsHd = null;
                StringBuilder blrRegistersValuesHd = null;
                // ===================== tag <CHANNELS> ==========================
                NodeList channels = doc.getElementsByTagName("Channel");
                logger.info("CHANNELS FOUND: " + channels.getLength());
                for (int i = 0; i < channels.getLength(); i++) {
                    blrCurvesValuesHd = new StringBuilder();
                    blrCurvesColumnsHd = new StringBuilder();
                    blrRegistersValuesHd = new StringBuilder();

                    Element channel = (Element) channels.item(i);
                    //String timezoneChannel = channel.getElementsByTagName("TimeZone").item(0).getTextContent();
                    String timezoneChannel = channel.getAttribute("TimeZone");
                    int intervalLength = Integer.parseInt(channel.getAttribute("IntervalLength"));//Mins

                    //Channels with IntervalLengths not in (-1 , 15) not processed. File is considered SUCCESS with not null message [contains Set of Invalid IntervalLengths
                    if ( intervalLength != -1 &&  intervalLength != 15  ) {
                        //throw new Exception("intervalLength=" + intervalLength);
                        setInvalidInterval.add(intervalLength);
                        continue;
                    }
                    String servicePoint = channel
                            .getElementsByTagName("ChannelID")
                            .item(0).getAttributes().getNamedItem("ServicePointChannelID").getTextContent();
                    //KVARTEST1:3  pomid:dataclass
                    //GR00000000880000000000:101  ==> GR0 000000088[SUPPLY_NUM] From char 4 up to  13 digits paroci (Perifereia + 8 digits paroxi) 0000000000
                    int  indxOf =  servicePoint.indexOf(":");
                    String podId = servicePoint.substring(0, indxOf);
                    String dataClass = servicePoint.substring(indxOf + 1 );
                    String supplyNum = null;
                    try{
                        supplyNum = servicePoint.substring(3, 12);
                    }
                    catch (Exception e){
                        supplyNum = "SUPPNUM";
                    }
                    String startDate = channel.getAttribute("StartDate");
                    String endDate = channel.getAttribute("EndDate");
                    String timeZone = channel.getAttribute("TimeZone");
                    String isRegister = channel.getAttribute("IsRegister");
                    if (intervalLength == 15) {
                        blrCurvesColumnsHd = blrCurvesColumnsHd.append("INSERT INTO SMC_MDM_SCCURVES ( POD_ID, METER_NO, SUPPLY_NUM, DATA_CLASS, HD_LOG_ID,");
                        //pending
                        blrCurvesValuesHd = blrCurvesValuesHd.append("'" + podId).append("',").append("null").append(",'").append(supplyNum).append("','").append(dataClass).append("',").append(fileId).append(",");
                    } else if (intervalLength == -1) {
                        //registers
                        blrRegistersColumnsHd = new StringBuilder();
                        blrRegistersColumnsHd = blrRegistersColumnsHd.append("INSERT INTO SMC_MDM_REGISTERS_DT (START_DATE, POD_ID, METER_NO, SUPPLY_NUM, DATA_CLASS, HD_LOG_ID,");
                        System.out.println(blrRegistersColumnsHd);
                        //pending
                        blrRegistersValuesHd = new StringBuilder();
                        blrRegistersValuesHd = blrRegistersValuesHd.append(formatToDate(startDate)).append(",'" + podId).append("',").append("null").append(",'").append(supplyNum).append("','").append(dataClass).append("',").append(fileId).append(",");
                        System.out.println(blrRegistersValuesHd);
                    }
                    /*
                    logger.info("ServicePointChannelID: " + servicePoint);
                    logger.info("StartDate: " + startDate);
                    logger.info("EndDate: " + endDate);
                    logger.info("IsRegister: " + isRegister);
                    */
                    logger.info("\nChannel " + (i + 1) + " servicePoint =" + servicePoint);
                    logger.info("IntervalLength: " + intervalLength);
                    //===========================parsing tag <TimePeriod>========================
                    // 1. immediatelly closes 2. is optional 3.exists FOR IntervalLength=15
                    NodeList nodeListTimePeriod = null;
                    if (intervalLength == 15) {
                        nodeListTimePeriod = channel.getElementsByTagName("TimePeriod");
                    }
                    String startDateTimeXML = "";
                    String startDateXML = "";
                    String endDateTimeXML = "";
                    String startTimeXML = "";
                    String endTimeXML = "";
                    OffsetDateTime odt = null;//add to time + 15min hh:mm:ss
                    OffsetDateTime oDate = null; //add to Date + 1d  dd/mm/yyyy
;                   //if (nodeListTimePeriod.getLength() != 0) {
                    if (nodeListTimePeriod != null) {
                        startDateTimeXML = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("StartTime").getTextContent();
                        startDateXML = startDateTimeXML.substring(0,10);
                        endDateTimeXML = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("EndTime").getTextContent();
                        //String startRead = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("StartRead").getTextContent();
                        //String endRead = channel.getElementsByTagName("TimePeriod").item(0).getAttributes().getNamedItem("EndRead").getTextContent();
                        logger.info("\nstartDateTimeXML =" + startDateTimeXML + " endDateTimeXML =" + endDateTimeXML );
                        logger.info("startDateXML =" + startDateXML);
                        startTimeXML = startDateTimeXML.substring(11,19);
                        endTimeXML = endDateTimeXML.substring(11,19);
                        logger.info("startTimeXML =" + startTimeXML+ " endTimeXML =" + endTimeXML);

                        odt = OffsetDateTime.parse(startDateTimeXML);
                        oDate = OffsetDateTime.parse(startDateTimeXML);
                    }
                    //Creating CURVES structure
                    //ChannelDAO channelDAO = new ChannelDAO(servicePoint, startDateTimeXML, endDateTimeXML, timeZone, intervalLength, isRegister);
                    StringBuilder blrCurvesColumns = new StringBuilder();
                    StringBuilder blrCurvesValues = new StringBuilder();
                    StringBuilder blrRegistersColumns = new StringBuilder();
                    StringBuilder blrRegistersValues = new StringBuilder();
                    //logger.info(blrCurvesColumns);
                    //logger.info(blrCurvesValues + " " + oDate);
                    /* ===================== READINGS ======================
                    ======================== READINGS ======================
                    ======================== READINGS ========================*/
                    NodeList readings = channel.getElementsByTagName("Reading");
                    logger.info("  Total Readings: " + readings.getLength());
                    int batchTotal = 0;
                    OffsetDateTime result = odt;
                    if (intervalLength == 15) {
                        curvesTimeCalc = twoDigits(result.getHour()) + ":" + twoDigits(result.getMinute()) + ":" + twoDigits(result.getSecond());
                    }
                    Boolean updAfterLoopFlg = null;
                    for (int j = 0; j < readings.getLength(); ++j) {
                        updAfterLoopFlg = true;
                        Element reading = (Element) readings.item(j);
                        String value = reading.getAttribute("Value");
                        String statusRef = reading.getAttribute("StatusRef");
                        String readingTime = reading.hasAttribute("ReadingTime")
                                ? reading.getAttribute("ReadingTime")
                                :null;
                        int assgnTimeToCurve = -1;
                        //curvesDateCalc = startDateXML;
                        if (intervalLength == -1) {
                            blrRegistersColumns = new StringBuilder();
                            blrRegistersValues = new StringBuilder();
                            blrRegistersColumns = blrRegistersColumns.append("Q").append(",");
                            blrRegistersValues = blrRegistersValues.append(value).append(",");
                            logger.info(String.valueOf(blrRegistersColumns));
                            logger.info(String.valueOf(blrRegistersValues));
                            blrRegistersColumns = blrRegistersColumns.append("S" ).append(",");
                            blrRegistersValues = blrRegistersValues.append("'").append(statusRef).append("',");
                            blrRegistersColumns = blrRegistersColumns.append("DATE_READ" ).append(") values (");

                            blrRegistersValues = blrRegistersValues.append(formatToDate(readingTime)).append(")");
                            //logger.info(blrRegistersColumns);
                            //logger.info(blrRegistersValues);
                            logger.info(blrRegistersColumnsHd.toString() + blrRegistersColumns.toString() + blrRegistersValuesHd.toString() + blrRegistersValues.toString());
                            //exec
                            psRegistersReadingsData = conn.prepareStatement(blrRegistersColumnsHd.toString() + blrRegistersColumns.toString() + blrRegistersValuesHd.toString() + blrRegistersValues.toString() );
                            psRegistersReadingsData.addBatch();
                            int[] resultRegistersReadingData = psRegistersReadingsData.executeBatch();
                        }
                        else if (intervalLength == 15) {
                            curvesTimeCalc = twoDigits(result.getHour()) + ":" + twoDigits(result.getMinute()) + ":" + twoDigits(result.getSecond());
                            assgnTimeToCurve = assignTimeToCurve(curvesTimeCalc);
                            result = result.plusMinutes(intervalLength);
                            blrCurvesColumns = blrCurvesColumns.append("Q" + assgnTimeToCurve).append(",");
                            blrCurvesValues = blrCurvesValues.append(value).append(",");
                            blrCurvesColumns = blrCurvesColumns.append("S" + assgnTimeToCurve).append(",");
                            blrCurvesValues = blrCurvesValues.append(statusRef).append(",");
                        }

                        //if (j==20) return 1; //pending
                        boolean dayChangedFlg = (assgnTimeToCurve == 96) ? true : false;
                        //logger.info("(" + (j + 1) + ") curvesTimeCalc=" + curvesTimeCalc+ " Q" + assgnTimeToCurve + " val=" + value + " dayChangedFlg=" + dayChangedFlg + " oDate=" + oDate );
                        if ( dayChangedFlg ) {
                            logger.info("Readings At chng day ctr :"+ j );
                            if ( j == 95) {
                                updAfterLoopFlg = false;
                            }
                            blrCurvesColumns = blrCurvesColumns.append("DATE_READ)").append(" VALUES(");
                            //here
                            logger.info(blrCurvesColumnsHd.toString() + blrCurvesColumns.toString()  +
                                                blrCurvesValuesHd.toString() + blrCurvesValues.toString() + "'" +
                                                oDate.format(formatter) + "')"
                            );
                            //exec
                            psCurvesReadingsDataRow = conn.prepareStatement(blrCurvesColumnsHd.toString() + blrCurvesColumns.toString()  +
                                    blrCurvesValuesHd.toString() + blrCurvesValues.toString() + "'" +
                                    oDate.format(formatter) + "')"
                            );
                            psCurvesReadingsDataRow.addBatch();
                            int[] resultCurvesDataRow = psCurvesReadingsDataRow.executeBatch();

                            oDate = oDate.plusDays(1);
                            blrCurvesColumns = new StringBuilder();
                            blrCurvesValues = new StringBuilder();
                        }
                        //logger.info("    Value=" + value + ", StatusRef=" + statusRef + ", Time=" + readingTime);
                        /*
                        Static member 'com.hedno.integration.dao.ConnectOracle.getPreparedStatement(java.lang.String)' accessed via instance reference
                        Inspection info: Reports references to static methods and fields via a class instance rather than the class itself.
                        Even though referring to static members via instance variables is allowed by The Java Language Specification,
                        this makes the code confusing as the reader may think that the result of the method depends on the instance.
                        The quick-fix replaces the instance variable with the class name.
                         */
                        //Insert into ITRON_FILE_READINGS
                        /*
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
                        */
                        //Creating CURVES structure
                        //ReadingDAO readingDAO = new ReadingDAO(BigDecimal.valueOf(Double.parseDouble(value)),statusRef,readingTime);
                        //ReadingDAO readingDAO = new ReadingDAO(Double.parseDouble(value),statusRef,readingTime);
                        //channelDAO.lstReadings.add( readingDAO);
                    }
                    //=============End Readings Loop
                    if ( intervalLength == 15) {
                        if (updAfterLoopFlg) {
                            blrCurvesColumns = blrCurvesColumns.append("DATE_READ)").append(" VALUES(");
                            logger.info(blrCurvesColumnsHd.toString() + blrCurvesColumns +
                                    blrCurvesValuesHd.toString() + blrCurvesValues.toString() + "'" + oDate.format(formatter) + "')"
                            );
                            //exec
                            psCurvesReadingsDataRow = conn.prepareStatement(blrCurvesColumnsHd.toString() + blrCurvesColumns.toString() +
                                    blrCurvesValuesHd.toString() + blrCurvesValues.toString() + "'" + oDate.format(formatter) + "')"
                            );
                            psCurvesReadingsDataRow.addBatch();
                            int[] resultCurvesDataRow = psCurvesReadingsDataRow.executeBatch();
                        }
                    }
                    //pending
                    //conn.commit();
                    //Printing Curves Data
                    //logger.info("\n" + channelDAO.servicePointChannelId);
                    //logger.info(channelDAO.lstReadings);
                }
                //=============END CHANNELS LOOP
                totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Duration :" + (totalDuration / 1000) / 60 + " Mins");
                logger.info("Duration :" + (totalDuration / 1000) + " Secs");
                logger.info("Duration :" + totalDuration + " Millisecs");
                //Update HEADER
                try{
                    psUpdProcess.setString(1, "SUCCESS" );
                    psUpdProcess.setString(2,  ( (setInvalidInterval.size() != 0)  ? setInvalidInterval.toString() : "") );
                    psUpdProcess.setLong(3, fileId);
                    psUpdProcess.addBatch();
                    int[] resultData = psUpdProcess.executeBatch();
                }
                catch (Exception ex){
                    ex.printStackTrace();
                    return -1;
                }
                return 0;
            }
            else{
                psUpdProcess.setString(1, "ERROR");
                psUpdProcess.setString(2, erroCoderMessgArr[1] );
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
                return -1;
            }
        }
        catch (NullPointerException nullPtrExc){
            nullPtrExc.printStackTrace();
            logger.info(nullPtrExc.getMessage());
            //Update Master Table (status, status_msg) with Failure
            try {
                psUpdProcess.setString(1, "ERROR");
                psUpdProcess.setString(2, "null Pointer");
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
                //Delete from Detail Table
                psDelCurvesReadings.setLong(1, fileId);
                psDelCurvesReadings.addBatch();
                resultData = psDelCurvesReadings.executeBatch();
            }
            catch (Exception exxxx){}
        }
        catch (Exception ee) {
            ee.printStackTrace();
            logger.info(ee.getMessage());
            try{
                //Update Master Table (status, status_msg) with Failure
                psUpdProcess.setString(1, "ERROR");
                psUpdProcess.setString(2, ee.getMessage());
                psUpdProcess.setLong(3, fileId);
                psUpdProcess.addBatch();
                int[] resultData = psUpdProcess.executeBatch();
                //Delete from Detail Table
                psDelCurvesReadings.setLong(1, fileId);
                psDelCurvesReadings.addBatch();
                resultData = psDelCurvesReadings.executeBatch();
            }
            catch (Exception ex){
                ex.printStackTrace();
            }
        }
        finally {
         	try {
				conn.close();
			} catch (Exception e) {
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
                                            logger.warn("Warning: " + e.getMessage());
                                        }
                                        @Override
                                        public void error(SAXParseException e) throws SAXException {
                                            logger.error("Error: " + e.getMessage());
                                            throw e; // stop parsing
                                        }
                                        @Override
                                        public void fatalError(SAXParseException e) throws SAXException {
                                            logger.error("Fatal: " + e.getMessage());
                                            throw e; // stop parsing
                                        }
                                    }
            );
            // Parse and automatically check well-formedness
            Document doc = builder.parse(xmlFile);
            //logger.info("XML is well-formed! Root element: " + doc.getDocumentElement().getNodeName()  + "\n");

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
        logger.info("in_xmlFile:" + in_xmlFile);
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
            //logger.info("\nxmlText :\n" + xmlText );
            //Add quotes around unquoted attribute values
            Matcher m = ATTR_NO_QUOTES.matcher(xmlText);
            StringBuffer sb = new StringBuffer();
            logger.info("entered");
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

            logger.info("Repaired XML written to: " + fixed.toAbsolutePath());

            // parsing the repaired file to confirm validity
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(fixed.toFile());
            logger.info("XML parsed successfully. Root element: " + doc.getDocumentElement().getNodeName());
            return 1;
        }
        catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }
}
