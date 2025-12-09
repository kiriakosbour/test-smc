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
 * - Generic interval data format
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class LoadProfileDataExtractor {

    private static final Logger logger = LoggerFactory.getLogger(LoadProfileDataExtractor.class);

    // XML namespaces
    private static final String NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/";
    private static final String NS_ZFA = "http://sap.com/xi/SAPGlobal20/Global";

    // Date formatters
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    private static final DateTimeFormatter ZFA_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

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

        // Find UtilitiesTimeSeriesERPItemBulkNotificationMessage elements
        NodeList messageNodes = doc.getElementsByTagNameNS(NS_ZFA, 
            "UtilitiesTimeSeriesERPItemBulkNotificationMessage");

        if (messageNodes.getLength() == 0) {
            // Try without namespace
            messageNodes = doc.getElementsByTagName("UtilitiesTimeSeriesERPItemBulkNotificationMessage");
        }

        logger.debug("Found {} message elements", messageNodes.getLength());

        for (int i = 0; i < messageNodes.getLength(); i++) {
            Element messageEl = (Element) messageNodes.item(i);
            LoadProfileData profile = extractProfile(messageEl, messageUuid);
            if (profile != null && !profile.getIntervals().isEmpty()) {
                profiles.add(profile);
            }
        }

        logger.info("Extracted {} load profiles from XML", profiles.size());
        return profiles;
    }

    /**
     * Extract message UUID from XML header
     */
    private String extractMessageUuid(Document doc) {
        // Try BusinessScope/ID
        NodeList uuidNodes = doc.getElementsByTagName("UUID");
        if (uuidNodes.getLength() > 0) {
            return uuidNodes.item(0).getTextContent().trim();
        }

        // Try MessageHeader/UUID
        NodeList headerNodes = doc.getElementsByTagName("MessageHeader");
        if (headerNodes.getLength() > 0) {
            Element header = (Element) headerNodes.item(0);
            NodeList idNodes = header.getElementsByTagName("UUID");
            if (idNodes.getLength() > 0) {
                return idNodes.item(0).getTextContent().trim();
            }
        }

        // Generate new UUID if not found
        return UUID.randomUUID().toString().toUpperCase();
    }

    /**
     * Extract single profile from message element
     */
    private LoadProfileData extractProfile(Element messageEl, String messageUuid) {
        LoadProfileData profile = new LoadProfileData();
        profile.setMessageUuid(messageUuid);

        try {
            // Extract POD ID (UtilitiesDeviceID or MeteringPointID)
            String podId = getElementText(messageEl, "UtilitiesDeviceID");
            if (podId == null || podId.isEmpty()) {
                podId = getElementText(messageEl, "MeteringPointID");
            }
            if (podId == null || podId.isEmpty()) {
                podId = getElementText(messageEl, "ServicePointChannelID");
            }
            profile.setPodId(podId);

            // Extract OBIS code / Data Class
            String obisCode = getElementText(messageEl, "UtilitiesMeasurementTaskTypeCode");
            if (obisCode == null || obisCode.isEmpty()) {
                obisCode = getElementText(messageEl, "MeasuredQuantityTypeCode");
            }
            profile.setObisCode(obisCode);

            // Extract intervals
            NodeList itemNodes = messageEl.getElementsByTagName("UtilitiesTimeSeriesItem");
            if (itemNodes.getLength() == 0) {
                itemNodes = messageEl.getElementsByTagName("IntervalReading");
            }

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
            logger.error("Error extracting profile", e);
            return null;
        }

        return profile;
    }

    /**
     * Extract single interval from item element
     */
    private IntervalData extractInterval(Element itemEl) {
        IntervalData interval = new IntervalData();

        try {
            // Extract start time
            String startTimeStr = getElementText(itemEl, "StartDateTime");
            if (startTimeStr == null || startTimeStr.isEmpty()) {
                startTimeStr = getElementText(itemEl, "UtilitiesTimeSeriesItemDateTime");
            }
            if (startTimeStr != null && !startTimeStr.isEmpty()) {
                interval.setStartDateTime(parseDateTime(startTimeStr));
            }

            // Extract value
            String valueStr = getElementText(itemEl, "Quantity");
            if (valueStr == null || valueStr.isEmpty()) {
                valueStr = getElementText(itemEl, "UtilitiesQuantityValue");
            }
            if (valueStr == null || valueStr.isEmpty()) {
                valueStr = getElementText(itemEl, "Value");
            }
            if (valueStr != null && !valueStr.isEmpty()) {
                try {
                    interval.setValue(new BigDecimal(valueStr.trim()));
                } catch (NumberFormatException e) {
                    logger.warn("Invalid value: {}", valueStr);
                    interval.setValue(BigDecimal.ZERO);
                }
            }

            // Extract unit
            String unit = getElementText(itemEl, "QuantityUnitCode");
            if (unit == null || unit.isEmpty()) {
                unit = getElementText(itemEl, "MeasureUnitCode");
            }
            interval.setUnitCode(unit != null ? unit : "KWH");

            // Extract status
            String status = getElementText(itemEl, "UtilitiesTimeSeriesItemStatusCode");
            if (status == null || status.isEmpty()) {
                status = getElementText(itemEl, "StatusRef");
            }
            interval.setStatus(status != null ? status : "W");

        } catch (Exception e) {
            logger.warn("Error extracting interval", e);
            return null;
        }

        return interval;
    }

    /**
     * Get text content of first matching child element
     */
    private String getElementText(Element parent, String tagName) {
        // Try with namespace first
        NodeList nodes = parent.getElementsByTagNameNS(NS_ZFA, tagName);
        if (nodes.getLength() == 0) {
            // Try without namespace
            nodes = parent.getElementsByTagName(tagName);
        }
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
            // Remove timezone suffix if present for parsing
            String cleaned = dateTimeStr.trim();
            
            // Try ISO format first
            if (cleaned.contains("T")) {
                if (cleaned.endsWith("Z")) {
                    return LocalDateTime.parse(cleaned, ZFA_FORMATTER);
                } else if (cleaned.contains("+") || cleaned.lastIndexOf("-") > 10) {
                    // Has timezone offset, parse as ISO
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
            logger.warn("Could not parse datetime: {}", dateTimeStr);
            return null;
        }
    }
}
