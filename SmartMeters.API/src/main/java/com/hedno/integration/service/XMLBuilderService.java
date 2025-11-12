package com.hedno.integration.service;

import com.hedno.integration.soap.model.UtilitiesTimeSeriesERPItemBulkNotification;
import com.hedno.integration.soap.model.UtilitiesTimeSeriesERPItemBulkNotification.*;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;

/**
 * Service for building and validating XML messages according to WSDL specifications.
 * 
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class XMLBuilderService {
    
    private static final Logger logger = LoggerFactory.getLogger(XMLBuilderService.class);
    
    // Fixed values as per specification
    private static final String SENDER_PARTY_ID = "T_MERES";
    private static final String RECIPIENT_PARTY_ID = "EHE000130";
    private static final String DEFAULT_ITEM_STATUS = "W";
    private static final String NAMESPACE = "http://sap.com/xi/SAPGlobal20/Global";
    
    // Default configuration values
    private static final int DEFAULT_MAX_PROFILES_PER_MESSAGE = 10;
    private static final int INTERVALS_PER_DAY = 96; // 15-minute intervals
    
    private final JAXBContext jaxbContext;
    private final DatatypeFactory datatypeFactory;
    private final TimeBasedGenerator uuidGenerator;
    private final int maxProfilesPerMessage;
    
    /**
     * Constructor with default max profiles per message
     */
    public XMLBuilderService() throws JAXBException, DatatypeConfigurationException {
        this(DEFAULT_MAX_PROFILES_PER_MESSAGE);
    }
    
    /**
     * Constructor with configurable max profiles per message
     */
    public XMLBuilderService(int maxProfilesPerMessage) throws JAXBException, DatatypeConfigurationException {
        this.jaxbContext = JAXBContext.newInstance(UtilitiesTimeSeriesERPItemBulkNotification.class);
        this.datatypeFactory = DatatypeFactory.newInstance();
        this.uuidGenerator = Generators.timeBasedGenerator();
        this.maxProfilesPerMessage = maxProfilesPerMessage;
        logger.info("XMLBuilderService initialized with max {} profiles per message", maxProfilesPerMessage);
    }
    
    /**
     * Build the bulk notification message
     * @param profileDataList List of profile data to include
     * @return UtilitiesTimeSeriesERPItemBulkNotification
     */
    public UtilitiesTimeSeriesERPItemBulkNotification buildBulkNotification(
            List<LoadProfileData> profileDataList) {
        
        if (profileDataList == null || profileDataList.isEmpty()) {
            throw new IllegalArgumentException("Profile data list cannot be null or empty");
        }
        
        if (profileDataList.size() > maxProfilesPerMessage) {
            throw new IllegalArgumentException(String.format(
                "Profile count %d exceeds maximum %d per message", 
                profileDataList.size(), maxProfilesPerMessage));
        }
        
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            new UtilitiesTimeSeriesERPItemBulkNotification();
        
        // Create main message header
        String mainUuid = generateUuid();
        notification.setMessageHeader(createMessageHeader(mainUuid));
        
        // Add individual notification messages for each profile
        for (LoadProfileData profileData : profileDataList) {
            notification.getNotificationMessages().add(
                createNotificationMessage(mainUuid, profileData));
        }
        
        logger.debug("Built bulk notification with {} profiles, UUID: {}", 
            profileDataList.size(), mainUuid);
        
        return notification;
    }
    
    /**
     * Create the main message header
     */
    private MessageHeader createMessageHeader(String uuid) {
        MessageHeader header = new MessageHeader();
        
        header.setUuid(uuid);
        header.setReferenceUuid(uuid);
        header.setCreationDateTime(getCurrentTimestamp());
        
        // Set sender party
        Party senderParty = new Party();
        senderParty.setStandardID(new Party.StandardID(SENDER_PARTY_ID));
        header.setSenderParty(senderParty);
        
        // Set recipient party
        Party recipientParty = new Party();
        recipientParty.setStandardID(new Party.StandardID(RECIPIENT_PARTY_ID));
        header.setRecipientParty(recipientParty);
        
        return header;
    }
    
    /**
     * Create individual notification message for a profile
     */
    private UtilitiesTimeSeriesERPItemNotificationMessage createNotificationMessage(
            String mainUuid, LoadProfileData profileData) {
        
        UtilitiesTimeSeriesERPItemNotificationMessage message = 
            new UtilitiesTimeSeriesERPItemNotificationMessage();
        
        // Set message header (same as main header)
        message.setMessageHeader(createMessageHeader(mainUuid));
        
        // Create utilities time series
        UtilitiesTimeSeries timeSeries = new UtilitiesTimeSeries();
        
        // Add time series items (one per interval)
        for (IntervalData interval : profileData.getIntervals()) {
            timeSeries.getItems().add(createTimeSeriesItem(interval));
        }
        
        // Set measurement role (meter identification)
        timeSeries.setMeasurementRole(createMeasurementRole(profileData));
        
        message.setUtilitiesTimeSeries(timeSeries);
        
        return message;
    }
    
    /**
     * Create individual time series item
     */
    private TimeSeriesItem createTimeSeriesItem(IntervalData interval) {
        TimeSeriesItem item = new TimeSeriesItem();
        
        // Set quantity with unit
        Quantity quantity = new Quantity(interval.getValue(), interval.getUnitCode());
        item.setQuantity(quantity);
        
        // Set validity period
        item.setUtcValidityStartDateTime(convertToXMLGregorianCalendar(interval.getStartDateTime()));
        item.setUtcValidityEndDateTime(convertToXMLGregorianCalendar(interval.getEndDateTime()));
        
        // Set status
        ItemStatus status = new ItemStatus();
        status.setUtilitiesTimeSeriesItemTypeCode(
            interval.getStatus() != null ? interval.getStatus() : DEFAULT_ITEM_STATUS);
        item.setItemStatus(status);
        
        return item;
    }
    
    /**
     * Create measurement role with meter identification
     */
    private UtilitiesMeasurementTaskAssignmentRole createMeasurementRole(LoadProfileData profileData) {
        UtilitiesMeasurementTaskAssignmentRole role = new UtilitiesMeasurementTaskAssignmentRole();
        
        role.setObisCode(profileData.getObisCode());
        
        UtilitiesMeasurementTaskAssignmentRole.PointOfDeliveryIdentification podId = 
            new UtilitiesMeasurementTaskAssignmentRole.PointOfDeliveryIdentification();
        podId.setPodId(profileData.getPodId());
        role.setPointOfDeliveryIdentification(podId);
        
        return role;
    }
    
    /**
     * Marshal the notification to XML string
     * @param notification The notification to marshal
     * @return XML string
     */
    public String marshalToXml(UtilitiesTimeSeriesERPItemBulkNotification notification) 
            throws JAXBException {
        
        StringWriter writer = new StringWriter();
        
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, 
            "http://sap.com/xi/SAPGlobal20/Global UtilitiesTimeSeriesERPItemBulkNotification_OutService.xsd");
        
        // Add SOAP envelope wrapper
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        writer.write("<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" ");
        writer.write("xmlns:glob=\"" + NAMESPACE + "\">\n");
        writer.write("  <soapenv:Header/>\n");
        writer.write("  <soapenv:Body>\n");
        
        // Marshal the notification
        marshaller.marshal(notification, writer);
        
        // Close SOAP envelope
        writer.write("  </soapenv:Body>\n");
        writer.write("</soapenv:Envelope>");
        
        String xml = writer.toString();
        logger.debug("Marshalled notification to XML, size: {} characters", xml.length());
        
        return xml;
    }
    
    /**
     * Unmarshal XML string to notification object
     * @param xml The XML string to unmarshal
     * @return UtilitiesTimeSeriesERPItemBulkNotification
     */
    public UtilitiesTimeSeriesERPItemBulkNotification unmarshalFromXml(String xml) 
            throws JAXBException {
        
        // Extract body content from SOAP envelope if present
        String bodyContent = extractBodyContent(xml);
        
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        StringReader reader = new StringReader(bodyContent);
        
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            (UtilitiesTimeSeriesERPItemBulkNotification) unmarshaller.unmarshal(reader);
        
        logger.debug("Unmarshalled XML to notification with {} profiles", 
            notification.getNotificationMessages().size());
        
        return notification;
    }
    
    /**
     * Extract the message UUID from XML
     * @param xml The XML string
     * @return The message UUID or null if not found
     */
    public String extractMessageUuid(String xml) {
        try {
            UtilitiesTimeSeriesERPItemBulkNotification notification = unmarshalFromXml(xml);
            if (notification.getMessageHeader() != null) {
                return notification.getMessageHeader().getUuid();
            }
        } catch (JAXBException e) {
            logger.error("Failed to extract UUID from XML", e);
        }
        return null;
    }
    
    /**
     * Validate XML against WSDL requirements
     * @param xml The XML to validate
     * @return true if valid, false otherwise
     */
    public boolean validateXml(String xml) {
        try {
            UtilitiesTimeSeriesERPItemBulkNotification notification = unmarshalFromXml(xml);
            
            // Validate main header
            if (notification.getMessageHeader() == null) {
                logger.error("Missing main MessageHeader");
                return false;
            }
            
            MessageHeader header = notification.getMessageHeader();
            
            // Validate UUID (36 characters)
            if (header.getUuid() == null || header.getUuid().length() != 36) {
                logger.error("Invalid UUID format");
                return false;
            }
            
            // Validate sender party
            if (header.getSenderParty() == null || 
                !SENDER_PARTY_ID.equals(header.getSenderParty().getStandardID().getValue())) {
                logger.error("Invalid sender party");
                return false;
            }
            
            // Validate recipient party
            if (header.getRecipientParty() == null || 
                !RECIPIENT_PARTY_ID.equals(header.getRecipientParty().getStandardID().getValue())) {
                logger.error("Invalid recipient party");
                return false;
            }
            
            // Validate notification messages
            if (notification.getNotificationMessages() == null || 
                notification.getNotificationMessages().isEmpty()) {
                logger.error("No notification messages found");
                return false;
            }
            
            if (notification.getNotificationMessages().size() > maxProfilesPerMessage) {
                logger.error("Too many profiles: {} > {}", 
                    notification.getNotificationMessages().size(), maxProfilesPerMessage);
                return false;
            }
            
            // Validate each notification message
            for (UtilitiesTimeSeriesERPItemNotificationMessage msg : notification.getNotificationMessages()) {
                if (!validateNotificationMessage(msg)) {
                    return false;
                }
            }
            
            logger.debug("XML validation successful");
            return true;
            
        } catch (Exception e) {
            logger.error("XML validation failed", e);
            return false;
        }
    }
    
    /**
     * Validate individual notification message
     */
    private boolean validateNotificationMessage(UtilitiesTimeSeriesERPItemNotificationMessage message) {
        if (message.getMessageHeader() == null) {
            logger.error("Missing MessageHeader in notification message");
            return false;
        }
        
        if (message.getUtilitiesTimeSeries() == null) {
            logger.error("Missing UtilitiesTimeSeries");
            return false;
        }
        
        UtilitiesTimeSeries timeSeries = message.getUtilitiesTimeSeries();
        
        // Validate items exist
        if (timeSeries.getItems() == null || timeSeries.getItems().isEmpty()) {
            logger.error("No time series items found");
            return false;
        }
        
        // Validate measurement role
        if (timeSeries.getMeasurementRole() == null) {
            logger.error("Missing UtilitiesMeasurementTaskAssignmentRole");
            return false;
        }
        
        UtilitiesMeasurementTaskAssignmentRole role = timeSeries.getMeasurementRole();
        
        if (role.getObisCode() == null || role.getObisCode().isEmpty()) {
            logger.error("Missing OBIS code");
            return false;
        }
        
        if (role.getPointOfDeliveryIdentification() == null || 
            role.getPointOfDeliveryIdentification().getPodId() == null) {
            logger.error("Missing POD ID");
            return false;
        }
        
        return true;
    }
    
    /**
     * Generate UUID in the correct format
     */
    private String generateUuid() {
        UUID uuid = uuidGenerator.generate();
        return uuid.toString().toUpperCase();
    }
    
    /**
     * Get current timestamp as XMLGregorianCalendar
     */
    private XMLGregorianCalendar getCurrentTimestamp() {
        try {
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTimeInMillis(System.currentTimeMillis());
            XMLGregorianCalendar xmlCalendar = datatypeFactory.newXMLGregorianCalendar(calendar);
            xmlCalendar.setTimezone(0); // UTC
            return xmlCalendar;
        } catch (Exception e) {
            logger.error("Failed to create XMLGregorianCalendar", e);
            return null;
        }
    }
    
    /**
     * Convert LocalDateTime to XMLGregorianCalendar
     */
    private XMLGregorianCalendar convertToXMLGregorianCalendar(LocalDateTime dateTime) {
        try {
            ZonedDateTime zdt = dateTime.atZone(ZoneOffset.UTC);
            GregorianCalendar calendar = GregorianCalendar.from(zdt);
            return datatypeFactory.newXMLGregorianCalendar(calendar);
        } catch (Exception e) {
            logger.error("Failed to convert LocalDateTime to XMLGregorianCalendar", e);
            return null;
        }
    }
    
    /**
     * Extract body content from SOAP envelope
     */
    private String extractBodyContent(String xml) {
        if (xml.contains("<soapenv:Body>") && xml.contains("</soapenv:Body>")) {
            int start = xml.indexOf("<soapenv:Body>") + "<soapenv:Body>".length();
            int end = xml.indexOf("</soapenv:Body>");
            return xml.substring(start, end).trim();
        }
        return xml;
    }
    
    /**
     * Data class for load profile data
     */
    public static class LoadProfileData {
        private String obisCode;
        private String podId;
        private List<IntervalData> intervals;
        
        // Getters and setters
        public String getObisCode() { return obisCode; }
        public void setObisCode(String obisCode) { this.obisCode = obisCode; }
        
        public String getPodId() { return podId; }
        public void setPodId(String podId) { this.podId = podId; }
        
        public List<IntervalData> getIntervals() { return intervals; }
        public void setIntervals(List<IntervalData> intervals) { this.intervals = intervals; }
    }
    
    /**
     * Data class for interval data
     */
    public static class IntervalData {
        private BigDecimal value;
        private String unitCode;
        private LocalDateTime startDateTime;
        private LocalDateTime endDateTime;
        private String status;
        
        // Getters and setters
        public BigDecimal getValue() { return value; }
        public void setValue(BigDecimal value) { this.value = value; }
        
        public String getUnitCode() { return unitCode; }
        public void setUnitCode(String unitCode) { this.unitCode = unitCode; }
        
        public LocalDateTime getStartDateTime() { return startDateTime; }
        public void setStartDateTime(LocalDateTime startDateTime) { this.startDateTime = startDateTime; }
        
        public LocalDateTime getEndDateTime() { return endDateTime; }
        public void setEndDateTime(LocalDateTime endDateTime) { this.endDateTime = endDateTime; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }
}
