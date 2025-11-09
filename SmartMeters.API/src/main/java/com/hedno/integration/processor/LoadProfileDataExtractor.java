package com.hedno.integration.processor;

import com.hedno.integration.service.XMLBuilderService.LoadProfileData;
import com.hedno.integration.service.XMLBuilderService.IntervalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Extracts load profile data from raw XML for processing.
 * This is used when we need to parse existing XML data from the database.
 * 
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class LoadProfileDataExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(LoadProfileDataExtractor.class);
    
    private static final String NAMESPACE = "http://sap.com/xi/SAPGlobal20/Global";
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    private final DocumentBuilderFactory documentBuilderFactory;
    
    public LoadProfileDataExtractor() {
        this.documentBuilderFactory = DocumentBuilderFactory.newInstance();
        this.documentBuilderFactory.setNamespaceAware(true);
    }
    
    /**
     * Extract load profile data from XML string
     * @param xml The XML string to extract from
     * @return List of LoadProfileData
     */
    public List<LoadProfileData> extractFromXml(String xml) {
        List<LoadProfileData> profileDataList = new ArrayList<>();
        
        try {
            // Parse XML
            DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
            Document document = builder.parse(new org.xml.sax.InputSource(new StringReader(xml)));
            document.getDocumentElement().normalize();
            
            // Find all UtilitiesTimeSeriesERPItemNotificationMessage elements
            NodeList messageNodes = document.getElementsByTagNameNS(NAMESPACE, 
                "UtilitiesTimeSeriesERPItemNotificationMessage");
            
            logger.debug("Found {} notification messages in XML", messageNodes.getLength());
            
            for (int i = 0; i < messageNodes.getLength(); i++) {
                Node messageNode = messageNodes.item(i);
                if (messageNode.getNodeType() == Node.ELEMENT_NODE) {
                    LoadProfileData profileData = extractProfileData((Element) messageNode);
                    if (profileData != null) {
                        profileDataList.add(profileData);
                    }
                }
            }
            
            logger.info("Extracted {} load profiles from XML", profileDataList.size());
            
        } catch (Exception e) {
            logger.error("Failed to extract load profile data from XML", e);
        }
        
        return profileDataList;
    }
    
    /**
     * Extract profile data from a single notification message element
     */
    private LoadProfileData extractProfileData(Element messageElement) {
        try {
            LoadProfileData profileData = new LoadProfileData();
            
            // Get UtilitiesTimeSeries element
            NodeList timeSeriesNodes = messageElement.getElementsByTagNameNS(NAMESPACE, "UtilitiesTimeSeries");
            if (timeSeriesNodes.getLength() == 0) {
                logger.warn("No UtilitiesTimeSeries found in notification message");
                return null;
            }
            
            Element timeSeriesElement = (Element) timeSeriesNodes.item(0);
            
            // Extract measurement role (OBIS code and POD ID)
            extractMeasurementRole(timeSeriesElement, profileData);
            
            // Extract interval data
            List<IntervalData> intervals = extractIntervals(timeSeriesElement);
            profileData.setIntervals(intervals);
            
            logger.debug("Extracted profile data - OBIS: {}, POD: {}, Intervals: {}", 
                profileData.getObisCode(), profileData.getPodId(), intervals.size());
            
            return profileData;
            
        } catch (Exception e) {
            logger.error("Failed to extract profile data from notification message", e);
            return null;
        }
    }
    
    /**
     * Extract measurement role information (OBIS code and POD ID)
     */
    private void extractMeasurementRole(Element timeSeriesElement, LoadProfileData profileData) {
        NodeList roleNodes = timeSeriesElement.getElementsByTagNameNS(NAMESPACE, 
            "UtilitiesMeasurementTaskAssignmentRole");
        
        if (roleNodes.getLength() > 0) {
            Element roleElement = (Element) roleNodes.item(0);
            
            // Extract OBIS code
            NodeList obisNodes = roleElement.getElementsByTagNameNS(NAMESPACE, 
                "UtilitiesObjectIdentificationSystemCodeText");
            if (obisNodes.getLength() > 0) {
                profileData.setObisCode(obisNodes.item(0).getTextContent().trim());
            }
            
            // Extract POD ID
            NodeList podNodes = roleElement.getElementsByTagNameNS(NAMESPACE, 
                "UtilitiesPointOfDeliveryPartyID");
            if (podNodes.getLength() > 0) {
                profileData.setPodId(podNodes.item(0).getTextContent().trim());
            }
        }
    }
    
    /**
     * Extract interval data from time series
     */
    private List<IntervalData> extractIntervals(Element timeSeriesElement) {
        List<IntervalData> intervals = new ArrayList<>();
        
        NodeList itemNodes = timeSeriesElement.getElementsByTagNameNS(NAMESPACE, "Item");
        
        for (int i = 0; i < itemNodes.getLength(); i++) {
            Node itemNode = itemNodes.item(i);
            if (itemNode.getNodeType() == Node.ELEMENT_NODE) {
                IntervalData interval = extractInterval((Element) itemNode);
                if (interval != null) {
                    intervals.add(interval);
                }
            }
        }
        
        return intervals;
    }
    
    /**
     * Extract single interval data
     */
    private IntervalData extractInterval(Element itemElement) {
        try {
            IntervalData interval = new IntervalData();
            
            // Extract quantity and unit
            NodeList quantityNodes = itemElement.getElementsByTagNameNS(NAMESPACE, "Quantity");
            if (quantityNodes.getLength() > 0) {
                Element quantityElement = (Element) quantityNodes.item(0);
                
                // Get value
                String valueText = quantityElement.getTextContent().trim();
                interval.setValue(new BigDecimal(valueText));
                
                // Get unit code
                String unitCode = quantityElement.getAttribute("unitCode");
                interval.setUnitCode(unitCode);
            }
            
            // Extract start datetime
            NodeList startNodes = itemElement.getElementsByTagNameNS(NAMESPACE, "UTCValidityStartDateTime");
            if (startNodes.getLength() > 0) {
                String startText = startNodes.item(0).getTextContent().trim();
                interval.setStartDateTime(parseDateTime(startText));
            }
            
            // Extract end datetime
            NodeList endNodes = itemElement.getElementsByTagNameNS(NAMESPACE, "UTCValidityEndDateTime");
            if (endNodes.getLength() > 0) {
                String endText = endNodes.item(0).getTextContent().trim();
                interval.setEndDateTime(parseDateTime(endText));
            }
            
            // Extract status
            NodeList statusNodes = itemElement.getElementsByTagNameNS(NAMESPACE, 
                "UtilitiesTimeSeriesItemTypeCode");
            if (statusNodes.getLength() > 0) {
                interval.setStatus(statusNodes.item(0).getTextContent().trim());
            }
            
            return interval;
            
        } catch (Exception e) {
            logger.error("Failed to extract interval data", e);
            return null;
        }
    }
    
    /**
     * Parse datetime string to LocalDateTime
     */
    private LocalDateTime parseDateTime(String dateTimeStr) {
        try {
            // Remove 'Z' and treat as UTC
            if (dateTimeStr.endsWith("Z")) {
                dateTimeStr = dateTimeStr.substring(0, dateTimeStr.length() - 1);
            }
            return LocalDateTime.parse(dateTimeStr);
        } catch (Exception e) {
            logger.error("Failed to parse datetime: {}", dateTimeStr, e);
            return null;
        }
    }
    
    /**
     * Validate that extracted data is complete and valid
     * @param profileData The profile data to validate
     * @return true if valid, false otherwise
     */
    public boolean validateProfileData(LoadProfileData profileData) {
        if (profileData == null) {
            return false;
        }
        
        // Check required fields
        if (profileData.getObisCode() == null || profileData.getObisCode().isEmpty()) {
            logger.error("Missing OBIS code");
            return false;
        }
        
        if (profileData.getPodId() == null || profileData.getPodId().isEmpty()) {
            logger.error("Missing POD ID");
            return false;
        }
        
        if (profileData.getIntervals() == null || profileData.getIntervals().isEmpty()) {
            logger.error("No interval data");
            return false;
        }
        
        // Validate intervals
        for (IntervalData interval : profileData.getIntervals()) {
            if (!validateInterval(interval)) {
                return false;
            }
        }
        
        // Check expected number of intervals (96 for normal day, 92 or 100 for DST change)
        int intervalCount = profileData.getIntervals().size();
        if (intervalCount != 96 && intervalCount != 92 && intervalCount != 100) {
            logger.warn("Unexpected number of intervals: {}", intervalCount);
        }
        
        return true;
    }
    
    /**
     * Validate single interval
     */
    private boolean validateInterval(IntervalData interval) {
        if (interval.getValue() == null) {
            logger.error("Interval missing value");
            return false;
        }
        
        if (interval.getUnitCode() == null || interval.getUnitCode().isEmpty()) {
            logger.error("Interval missing unit code");
            return false;
        }
        
        if (interval.getStartDateTime() == null) {
            logger.error("Interval missing start datetime");
            return false;
        }
        
        if (interval.getEndDateTime() == null) {
            logger.error("Interval missing end datetime");
            return false;
        }
        
        // Validate that end is after start
        if (!interval.getEndDateTime().isAfter(interval.getStartDateTime())) {
            logger.error("Invalid interval period: start {} is not before end {}", 
                interval.getStartDateTime(), interval.getEndDateTime());
            return false;
        }
        
        return true;
    }
    
    /**
     * Calculate statistics for the profile data
     */
    public ProfileStatistics calculateStatistics(LoadProfileData profileData) {
        ProfileStatistics stats = new ProfileStatistics();
        
        if (profileData == null || profileData.getIntervals() == null) {
            return stats;
        }
        
        stats.setIntervalCount(profileData.getIntervals().size());
        
        BigDecimal total = BigDecimal.ZERO;
        BigDecimal min = null;
        BigDecimal max = null;
        
        for (IntervalData interval : profileData.getIntervals()) {
            BigDecimal value = interval.getValue();
            if (value != null) {
                total = total.add(value);
                
                if (min == null || value.compareTo(min) < 0) {
                    min = value;
                }
                
                if (max == null || value.compareTo(max) > 0) {
                    max = value;
                }
            }
        }
        
        stats.setTotalConsumption(total);
        stats.setMinValue(min);
        stats.setMaxValue(max);
        
        if (profileData.getIntervals().size() > 0) {
            stats.setAverageValue(total.divide(
                new BigDecimal(profileData.getIntervals().size()), 
                6, BigDecimal.ROUND_HALF_UP));
        }
        
        return stats;
    }
    
    /**
     * Statistics class for profile data
     */
    public static class ProfileStatistics {
        private int intervalCount;
        private BigDecimal totalConsumption;
        private BigDecimal averageValue;
        private BigDecimal minValue;
        private BigDecimal maxValue;
        
        // Getters and setters
        public int getIntervalCount() { return intervalCount; }
        public void setIntervalCount(int intervalCount) { this.intervalCount = intervalCount; }
        
        public BigDecimal getTotalConsumption() { return totalConsumption; }
        public void setTotalConsumption(BigDecimal totalConsumption) { 
            this.totalConsumption = totalConsumption; 
        }
        
        public BigDecimal getAverageValue() { return averageValue; }
        public void setAverageValue(BigDecimal averageValue) { this.averageValue = averageValue; }
        
        public BigDecimal getMinValue() { return minValue; }
        public void setMinValue(BigDecimal minValue) { this.minValue = minValue; }
        
        public BigDecimal getMaxValue() { return maxValue; }
        public void setMaxValue(BigDecimal maxValue) { this.maxValue = maxValue; }
        
        @Override
        public String toString() {
            return String.format("ProfileStatistics[intervals=%d, total=%s, avg=%s, min=%s, max=%s]",
                intervalCount, totalConsumption, averageValue, minValue, maxValue);
        }
    }
}
