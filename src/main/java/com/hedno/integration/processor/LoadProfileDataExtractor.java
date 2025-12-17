package com.hedno.integration.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Load Profile Data Extractor.
 * Parses MDM XML and extracts load profile data.
 * 
 * Supports:
 * - ZFA format (UtilitiesTimeSeriesERPItemBulkNotification)
 * - UtilitiesTimeSeriesERPItemNotificationMessage format
 * - Generic interval data format
 * 
 * @author HEDNO Integration Team
 * @version 3.1 - Fixed to handle actual XML structure
 */
public class LoadProfileDataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(LoadProfileDataExtractor.class);

    // XML namespaces
    private static final String NS_SAP_GLOBAL = "http://sap.com/xi/SAPGlobal20/Global";

    // Date formatters
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private final DocumentBuilderFactory factory;

    public LoadProfileDataExtractor() {
        this.factory = DocumentBuilderFactory.newInstance();
        this.factory.setNamespaceAware(true);
        this.factory.setIgnoringComments(true);
        this.factory.setIgnoringElementContentWhitespace(true);
    }

    /**
     * Extract load profiles from XML string
     * 
     * @param xmlContent The XML content
     * @return List of extracted load profiles
     */
    public List<LoadProfileData> extractFromXml(String xmlContent) throws Exception {
        List<LoadProfileData> profiles = new ArrayList<>();

        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new InputSource(new StringReader(xmlContent)));
        doc.getDocumentElement().normalize();

        // Try to extract message UUID from header
        String messageUuid = extractMessageUuid(doc);
        logger.debug("Extracted message UUID: {}", messageUuid);

        // Strategy 1: Find UtilitiesTimeSeriesERPItemNotificationMessage elements
        NodeList messageNodes = findMessageNodes(doc);
        logger.debug("Found {} notification message elements", messageNodes.getLength());

        if (messageNodes.getLength() > 0) {
            for (int i = 0; i < messageNodes.getLength(); i++) {
                Element messageEl = (Element) messageNodes.item(i);
                LoadProfileData profile = extractProfileFromNotificationMessage(messageEl, messageUuid);
                if (profile != null && !profile.getIntervals().isEmpty()) {
                    profiles.add(profile);
                    logger.debug("Extracted profile: POD={}, intervals={}", 
                        profile.getPodId(), profile.getIntervals().size());
                }
            }
        }

        // Strategy 2: If no profiles found, try direct UtilitiesTimeSeries extraction
        if (profiles.isEmpty()) {
            logger.debug("No profiles from notification messages, trying direct TimeSeries extraction");
            NodeList timeSeriesNodes = doc.getElementsByTagName("UtilitiesTimeSeries");
            if (timeSeriesNodes.getLength() == 0) {
                timeSeriesNodes = doc.getElementsByTagNameNS(NS_SAP_GLOBAL, "UtilitiesTimeSeries");
            }
            
            for (int i = 0; i < timeSeriesNodes.getLength(); i++) {
                Element tsEl = (Element) timeSeriesNodes.item(i);
                LoadProfileData profile = extractProfileFromTimeSeries(tsEl, messageUuid);
                if (profile != null && !profile.getIntervals().isEmpty()) {
                    profiles.add(profile);
                }
            }
        }

        logger.info("Extracted {} load profiles from XML", profiles.size());
        return profiles;
    }

    /**
     * Find notification message nodes with various namespace combinations
     */
    private NodeList findMessageNodes(Document doc) {
        // Try different element names and namespace combinations
        String[] elementNames = {
            "UtilitiesTimeSeriesERPItemNotificationMessage",
            "UtilitiesTimeSeriesERPItemBulkNotificationMessage"
        };
        
        for (String elementName : elementNames) {
            // Try with SAP Global namespace
            NodeList nodes = doc.getElementsByTagNameNS(NS_SAP_GLOBAL, elementName);
            if (nodes.getLength() > 0) {
                logger.debug("Found {} elements with name {} (NS: SAP Global)", nodes.getLength(), elementName);
                return nodes;
            }
            
            // Try without namespace
            nodes = doc.getElementsByTagName(elementName);
            if (nodes.getLength() > 0) {
                logger.debug("Found {} elements with name {} (no NS)", nodes.getLength(), elementName);
                return nodes;
            }
        }
        
        // Return empty NodeList
        return doc.getElementsByTagName("__NONEXISTENT__");
    }

    /**
     * Extract message UUID from XML header
     */
    private String extractMessageUuid(Document doc) {
        // Try different locations for UUID
        String[] uuidTags = {"UUID", "uuid", "MessageID", "messageId"};
        
        for (String tag : uuidTags) {
            NodeList uuidNodes = doc.getElementsByTagName(tag);
            if (uuidNodes.getLength() > 0) {
                String uuid = uuidNodes.item(0).getTextContent().trim();
                if (uuid != null && !uuid.isEmpty()) {
                    return uuid;
                }
            }
        }

        // Generate new UUID if not found
        String generated = UUID.randomUUID().toString().toUpperCase();
        logger.debug("No UUID found in XML, generated: {}", generated);
        return generated;
    }

    /**
     * Extract profile from UtilitiesTimeSeriesERPItemNotificationMessage element
     */
    private LoadProfileData extractProfileFromNotificationMessage(Element messageEl, String messageUuid) {
        // Find UtilitiesTimeSeries within this message
        NodeList tsNodes = messageEl.getElementsByTagName("UtilitiesTimeSeries");
        if (tsNodes.getLength() == 0) {
            tsNodes = messageEl.getElementsByTagNameNS(NS_SAP_GLOBAL, "UtilitiesTimeSeries");
        }
        
        if (tsNodes.getLength() == 0) {
            logger.warn("No UtilitiesTimeSeries found in notification message");
            return null;
        }
        
        Element tsEl = (Element) tsNodes.item(0);
        return extractProfileFromTimeSeries(tsEl, messageUuid);
    }

    /**
     * Extract profile from UtilitiesTimeSeries element
     */
    private LoadProfileData extractProfileFromTimeSeries(Element tsEl, String messageUuid) {
        LoadProfileData profile = new LoadProfileData();
        profile.setMessageUuid(messageUuid);

        try {
            // Extract POD ID from UtilitiesMeasurementTaskAssignmentRole
            String podId = extractPodId(tsEl);
            profile.setPodId(podId);

            // Extract OBIS code
            String obisCode = extractObisCode(tsEl);
            profile.setObisCode(obisCode);

            // Extract intervals from Item elements
            NodeList itemNodes = tsEl.getElementsByTagName("Item");
            logger.debug("Found {} Item elements", itemNodes.getLength());

            for (int i = 0; i < itemNodes.getLength(); i++) {
                Element itemEl = (Element) itemNodes.item(i);
                IntervalData interval = extractInterval(itemEl);
                if (interval != null) {
                    profile.addInterval(interval);
                }
            }

            logger.debug("Extracted profile: POD={}, OBIS={}, intervals={}",
                podId, obisCode, profile.getIntervals().size());

        } catch (Exception e) {
            logger.error("Error extracting profile from TimeSeries", e);
            return null;
        }

        return profile;
    }

    /**
     * Extract POD ID from various possible locations
     */
    private String extractPodId(Element parent) {
        // Priority order of tags to check
        String[] podTags = {
            "UtilitiesPointOfDeliveryPartyID",      // Found in test.xml
            "UtilitiesDeviceID",
            "MeteringPointID", 
            "ServicePointChannelID",
            "POD_ID",
            "PodId"
        };
        
        for (String tag : podTags) {
            String value = getElementTextDeep(parent, tag);
            if (value != null && !value.isEmpty()) {
                logger.debug("Found POD ID using tag {}: {}", tag, value);
                return value;
            }
        }
        
        logger.warn("No POD ID found in XML");
        return "UNKNOWN";
    }

    /**
     * Extract OBIS code from various possible locations
     */
    private String extractObisCode(Element parent) {
        // Priority order of tags to check
        String[] obisTags = {
            "UtilitiesObjectIdentificationSystemCodeText",  // Found in test.xml
            "UtilitiesMeasurementTaskTypeCode",
            "MeasuredQuantityTypeCode",
            "ObisCode",
            "OBIS"
        };
        
        for (String tag : obisTags) {
            String value = getElementTextDeep(parent, tag);
            if (value != null && !value.isEmpty()) {
                logger.debug("Found OBIS code using tag {}: {}", tag, value);
                return value;
            }
        }
        
        logger.warn("No OBIS code found in XML");
        return "UNKNOWN";
    }

    /**
     * Extract single interval from Item element
     */
    private IntervalData extractInterval(Element itemEl) {
        IntervalData interval = new IntervalData();

        try {
            // Extract start time - try multiple tag names
            String startTimeStr = getElementText(itemEl, "UTCValidityStartDateTime");
            if (startTimeStr == null || startTimeStr.isEmpty()) {
                startTimeStr = getElementText(itemEl, "StartDateTime");
            }
            if (startTimeStr == null || startTimeStr.isEmpty()) {
                startTimeStr = getElementText(itemEl, "UtilitiesTimeSeriesItemDateTime");
            }
            
            if (startTimeStr != null && !startTimeStr.isEmpty()) {
                interval.setStartDateTime(parseDateTime(startTimeStr));
            } else {
                logger.warn("No start time found in Item element");
                return null;
            }

            // Extract value from Quantity element
            Element quantityEl = getFirstChildElement(itemEl, "Quantity");
            if (quantityEl != null) {
                String valueStr = quantityEl.getTextContent().trim();
                try {
                    interval.setValue(new BigDecimal(valueStr));
                } catch (NumberFormatException e) {
                    logger.warn("Invalid quantity value: {}", valueStr);
                    interval.setValue(BigDecimal.ZERO);
                }
                
                // Extract unit from attribute
                String unit = quantityEl.getAttribute("unitCode");
                interval.setUnitCode(unit != null && !unit.isEmpty() ? unit : "KWH");
            } else {
                // Try direct Value element
                String valueStr = getElementText(itemEl, "Value");
                if (valueStr != null && !valueStr.isEmpty()) {
                    try {
                        interval.setValue(new BigDecimal(valueStr.trim()));
                    } catch (NumberFormatException e) {
                        interval.setValue(BigDecimal.ZERO);
                    }
                }
                interval.setUnitCode("KWH");
            }

            // Extract status from ItemStatus
            String status = extractStatus(itemEl);
            interval.setStatus(status != null ? status : "W");

        } catch (Exception e) {
            logger.warn("Error extracting interval", e);
            return null;
        }

        return interval;
    }

    /**
     * Extract status from ItemStatus element
     */
    private String extractStatus(Element itemEl) {
        // Try to find ItemStatus/UtilitiesTimeSeriesItemTypeCode
        Element statusEl = getFirstChildElement(itemEl, "ItemStatus");
        if (statusEl != null) {
            String status = getElementText(statusEl, "UtilitiesTimeSeriesItemTypeCode");
            if (status != null && !status.isEmpty()) {
                return status;
            }
        }
        
        // Try direct StatusRef
        String status = getElementText(itemEl, "StatusRef");
        if (status != null && !status.isEmpty()) {
            return status;
        }
        
        return "W"; // Default status
    }

    /**
     * Get first child element with given tag name
     */
    private Element getFirstChildElement(Element parent, String tagName) {
        NodeList nodes = parent.getElementsByTagName(tagName);
        if (nodes.getLength() > 0) {
            return (Element) nodes.item(0);
        }
        return null;
    }

    /**
     * Get text content of first matching child element (direct children only)
     */
    private String getElementText(Element parent, String tagName) {
        NodeList nodes = parent.getElementsByTagName(tagName);
        if (nodes.getLength() > 0) {
            String text = nodes.item(0).getTextContent();
            return text != null ? text.trim() : null;
        }
        return null;
    }

    /**
     * Get text content searching deeply in the element tree
     */
    private String getElementTextDeep(Element parent, String tagName) {
        // Try without namespace first
        NodeList nodes = parent.getElementsByTagName(tagName);
        if (nodes.getLength() > 0) {
            String text = nodes.item(0).getTextContent();
            return text != null ? text.trim() : null;
        }
        
        // Try with SAP Global namespace
        nodes = parent.getElementsByTagNameNS(NS_SAP_GLOBAL, tagName);
        if (nodes.getLength() > 0) {
            String text = nodes.item(0).getTextContent();
            return text != null ? text.trim() : null;
        }
        
        return null;
    }

    /**
     * Parse datetime string to LocalDateTime
     */
    private LocalDateTime parseDateTime(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.isEmpty()) {
            return null;
        }

        try {
            String cleaned = dateTimeStr.trim();
            
            // Handle UTC format: 2025-11-23T22:00:00Z
            if (cleaned.endsWith("Z")) {
                return LocalDateTime.parse(cleaned, UTC_FORMATTER);
            }
            
            // Handle ISO format with timezone offset
            if (cleaned.contains("T")) {
                if (cleaned.contains("+") || cleaned.lastIndexOf("-") > 10) {
                    // Has timezone offset, take first 19 chars
                    return LocalDateTime.parse(cleaned.substring(0, 19), 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                } else {
                    return LocalDateTime.parse(cleaned, ISO_FORMATTER);
                }
            }

            // Try simple format
            return LocalDateTime.parse(cleaned, 
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        } catch (Exception e) {
            logger.warn("Could not parse datetime: {} - {}", dateTimeStr, e.getMessage());
            return null;
        }
    }
}
