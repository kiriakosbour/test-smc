package com.hedno.integration.service;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Simplified XML Builder Service for Load Profile Push
 * Creates simpler XML format as per latest requirements
 * 
 * @author HEDNO Integration Team
 * @version 2.0
 */
public class SimpleXMLBuilderService {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleXMLBuilderService.class);
    
    private static final String SENDER_ID = "T_MERES";
    private static final String RECIPIENT_ID = "EHE000130";
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    private final DocumentBuilderFactory documentBuilderFactory;
    private final TransformerFactory transformerFactory;
    private final TimeBasedGenerator uuidGenerator;
    
    /**
     * Constructor
     */
    public SimpleXMLBuilderService() {
        this.documentBuilderFactory = DocumentBuilderFactory.newInstance();
        this.transformerFactory = TransformerFactory.newInstance();
        this.uuidGenerator = Generators.timeBasedGenerator();
        logger.info("SimpleXMLBuilderService initialized");
    }
    
    /**
     * Build a simple load profile message
     * @param profiles List of profile data
     * @return XML string
     */
    public String buildLoadProfileMessage(List<ProfileData> profiles) throws Exception {
        DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
        Document doc = builder.newDocument();
        
        // Generate UUID for this message
        String messageUuid = generateUuid();
        
        // Create root element
        Element root = doc.createElement("LoadProfilePush");
        root.setAttribute("xmlns", "http://hedno.gr/loadprofile");
        doc.appendChild(root);
        
        // Add header
        Element header = doc.createElement("Header");
        root.appendChild(header);
        
        addElement(doc, header, "MessageID", messageUuid);
        addElement(doc, header, "Timestamp", LocalDateTime.now().format(ISO_FORMATTER));
        addElement(doc, header, "Sender", SENDER_ID);
        addElement(doc, header, "Recipient", RECIPIENT_ID);
        addElement(doc, header, "ProfileCount", String.valueOf(profiles.size()));
        
        // Add profiles
        Element profilesElement = doc.createElement("Profiles");
        root.appendChild(profilesElement);
        
        for (ProfileData profile : profiles) {
            Element profileElement = createProfileElement(doc, profile);
            profilesElement.appendChild(profileElement);
        }
        
        // Convert to string
        String xml = documentToString(doc);
        
        // Log header info and count (NOT the full XML)
        logger.info("Built LoadProfile XML - MessageID: {}, ProfileCount: {}", 
            messageUuid, profiles.size());
        logger.debug("XML size: {} bytes", xml.getBytes().length);
        
        return xml;
    }
    
    /**
     * Build a SOAP envelope with the load profile message
     * @param profiles List of profile data
     * @return SOAP XML string
     */
    public String buildSoapMessage(List<ProfileData> profiles) throws Exception {
        DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
        Document doc = builder.newDocument();
        
        String messageUuid = generateUuid();
        
        // Create SOAP Envelope
        Element envelope = doc.createElementNS("http://schemas.xmlsoap.org/soap/envelope/", "soapenv:Envelope");
        envelope.setAttribute("xmlns:soapenv", "http://schemas.xmlsoap.org/soap/envelope/");
        envelope.setAttribute("xmlns:hed", "http://hedno.gr/loadprofile");
        doc.appendChild(envelope);
        
        // Add SOAP Header
        Element soapHeader = doc.createElementNS("http://schemas.xmlsoap.org/soap/envelope/", "soapenv:Header");
        envelope.appendChild(soapHeader);
        
        // Add message header to SOAP header
        Element msgHeader = doc.createElement("hed:MessageHeader");
        soapHeader.appendChild(msgHeader);
        addElement(doc, msgHeader, "MessageID", messageUuid);
        addElement(doc, msgHeader, "Timestamp", LocalDateTime.now().format(ISO_FORMATTER));
        
        // Add SOAP Body
        Element soapBody = doc.createElementNS("http://schemas.xmlsoap.org/soap/envelope/", "soapenv:Body");
        envelope.appendChild(soapBody);
        
        // Add LoadProfilePush to body
        Element loadProfilePush = doc.createElement("hed:LoadProfilePush");
        soapBody.appendChild(loadProfilePush);
        
        // Add header info
        Element header = doc.createElement("Header");
        loadProfilePush.appendChild(header);
        addElement(doc, header, "MessageID", messageUuid);
        addElement(doc, header, "ProfileCount", String.valueOf(profiles.size()));
        
        // Add profiles
        Element profilesElement = doc.createElement("Profiles");
        loadProfilePush.appendChild(profilesElement);
        
        for (ProfileData profile : profiles) {
            Element profileElement = createProfileElement(doc, profile);
            profilesElement.appendChild(profileElement);
        }
        
        // Convert to string
        String xml = documentToString(doc);
        
        // Log only headers and count
        logger.info("Built SOAP Message - MessageID: {}, ProfileCount: {}, Size: {} bytes", 
            messageUuid, profiles.size(), xml.getBytes().length);
        
        return xml;
    }
    
    /**
     * Create a profile element
     */
    private Element createProfileElement(Document doc, ProfileData profile) {
        Element profileElement = doc.createElement("Profile");
        
        addElement(doc, profileElement, "POD_ID", profile.getPodId());
        addElement(doc, profileElement, "ObisCode", profile.getObisCode());
        addElement(doc, profileElement, "MeterID", profile.getMeterId());
        addElement(doc, profileElement, "StartTime", profile.getStartTime().format(ISO_FORMATTER));
        addElement(doc, profileElement, "EndTime", profile.getEndTime().format(ISO_FORMATTER));
        
        // Add intervals
        Element intervalsElement = doc.createElement("Intervals");
        profileElement.appendChild(intervalsElement);
        
        for (IntervalData interval : profile.getIntervals()) {
            Element intervalElement = doc.createElement("Interval");
            
            addElement(doc, intervalElement, "Start", interval.getStart().format(ISO_FORMATTER));
            addElement(doc, intervalElement, "End", interval.getEnd().format(ISO_FORMATTER));
            addElement(doc, intervalElement, "Value", interval.getValue().toPlainString());
            addElement(doc, intervalElement, "Unit", interval.getUnit());
            addElement(doc, intervalElement, "Status", interval.getStatus());
            
            intervalsElement.appendChild(intervalElement);
        }
        
        return profileElement;
    }
    
    /**
     * Extract message UUID from XML
     */
    public String extractMessageId(String xml) {
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));
            
            // Try different possible locations
            String[] tagNames = {"MessageID", "MessageId", "UUID", "messageUuid"};
            for (String tagName : tagNames) {
                org.w3c.dom.NodeList nodeList = doc.getElementsByTagName(tagName);
                if (nodeList.getLength() > 0) {
                    return nodeList.item(0).getTextContent();
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract message ID from XML", e);
        }
        return null;
    }
    
    /**
     * Count profiles in XML
     */
    public int countProfiles(String xml) {
        try {
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));
            
            org.w3c.dom.NodeList profileNodes = doc.getElementsByTagName("Profile");
            return profileNodes.getLength();
        } catch (Exception e) {
            logger.error("Failed to count profiles in XML", e);
            return -1;
        }
    }
    
    /**
     * Validate XML structure
     */
    public boolean validateXml(String xml) {
        try {
            // Basic validation - parse the XML
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(new java.io.ByteArrayInputStream(xml.getBytes()));
            
            // Check for required elements
            String messageId = extractMessageId(xml);
            if (messageId == null || messageId.isEmpty()) {
                logger.error("Missing Message ID in XML");
                return false;
            }
            
            int profileCount = countProfiles(xml);
            if (profileCount <= 0) {
                logger.error("No profiles found in XML");
                return false;
            }
            
            logger.debug("XML validation passed - MessageID: {}, Profiles: {}", messageId, profileCount);
            return true;
            
        } catch (Exception e) {
            logger.error("XML validation failed", e);
            return false;
        }
    }
    
    // Helper methods
    
    private void addElement(Document doc, Element parent, String name, String value) {
        Element element = doc.createElement(name);
        element.setTextContent(value);
        parent.appendChild(element);
    }
    
    private String documentToString(Document doc) throws Exception {
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }
    
    private String generateUuid() {
        UUID uuid = uuidGenerator.generate();
        return uuid.toString().toUpperCase();
    }
    
    // Data classes
    
    public static class ProfileData {
        private String podId;
        private String obisCode;
        private String meterId;
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private List<IntervalData> intervals;
        
        // Getters and setters
        public String getPodId() { return podId; }
        public void setPodId(String podId) { this.podId = podId; }
        
        public String getObisCode() { return obisCode; }
        public void setObisCode(String obisCode) { this.obisCode = obisCode; }
        
        public String getMeterId() { return meterId; }
        public void setMeterId(String meterId) { this.meterId = meterId; }
        
        public LocalDateTime getStartTime() { return startTime; }
        public void setStartTime(LocalDateTime startTime) { this.startTime = startTime; }
        
        public LocalDateTime getEndTime() { return endTime; }
        public void setEndTime(LocalDateTime endTime) { this.endTime = endTime; }
        
        public List<IntervalData> getIntervals() { return intervals; }
        public void setIntervals(List<IntervalData> intervals) { this.intervals = intervals; }
    }
    
    public static class IntervalData {
        private LocalDateTime start;
        private LocalDateTime end;
        private BigDecimal value;
        private String unit;
        private String status;
        
        // Getters and setters
        public LocalDateTime getStart() { return start; }
        public void setStart(LocalDateTime start) { this.start = start; }
        
        public LocalDateTime getEnd() { return end; }
        public void setEnd(LocalDateTime end) { this.end = end; }
        
        public BigDecimal getValue() { return value; }
        public void setValue(BigDecimal value) { this.value = value; }
        
        public String getUnit() { return unit; }
        public void setUnit(String unit) { this.unit = unit; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }
}
