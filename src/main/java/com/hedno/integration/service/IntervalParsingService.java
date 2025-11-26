package com.hedno.integration.service;

import com.hedno.integration.processor.IntervalData;
import com.hedno.integration.processor.LoadProfileData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Robustly handles both Namespaced and Unqualified child elements.
 */
public class IntervalParsingService {

    private static final Logger logger = LoggerFactory.getLogger(IntervalParsingService.class);
    
    private final DocumentBuilderFactory factory;
    private final String defaultStatus;

    public IntervalParsingService() {
        this.factory = DocumentBuilderFactory.newInstance();
        this.factory.setNamespaceAware(true); 
        this.defaultStatus = System.getProperty("data.default.status", "W");
    }

    /**
     * Parses the Bulk Notification XML and returns a list of all contained profiles.
     */
    public List<LoadProfileData> parseAllProfiles(String rawXml) {
        List<LoadProfileData> profiles = new ArrayList<>();
        if (rawXml == null || rawXml.isEmpty()) {
            return profiles;
        }

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(rawXml)));
            doc.getDocumentElement().normalize();

            // Use "*" for namespace to match elements regardless of prefix/namespace qualification
            NodeList msgNodes = doc.getElementsByTagNameNS("*", "UtilitiesTimeSeriesERPItemNotificationMessage");

            for (int i = 0; i < msgNodes.getLength(); i++) {
                Node msgNode = msgNodes.item(i);
                if (msgNode.getNodeType() == Node.ELEMENT_NODE) {
                    LoadProfileData profile = parseSingleProfile((Element) msgNode);
                    if (profile != null) {
                        profiles.add(profile);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse Bulk XML", e);
        }
        return profiles;
    }

    /**
     * Legacy support for extracting a single profile by ID.
     */
    public List<IntervalData> parseIntervalsFromXml(String rawXml, String profilBlocId) {
        if (profilBlocId == null) return Collections.emptyList();
        
        return parseAllProfiles(rawXml).stream()
                .filter(p -> profilBlocId.equals(p.getProfilBlocId()))
                .findFirst()
                .map(LoadProfileData::getIntervals)
                .orElse(Collections.emptyList());
    }

    private LoadProfileData parseSingleProfile(Element msgElement) {
        try {
            LoadProfileData data = new LoadProfileData();

            // 1. Extract Header Info (UUID as ProfilBlocId)
            Element header = getChild(msgElement, "MessageHeader");
            if (header != null) {
                data.setProfilBlocId(getText(header, "UUID"));
            }

            // 2. Extract Measurement Role (OBIS & POD)
            Element timeSeries = getChild(msgElement, "UtilitiesTimeSeries");
            if (timeSeries == null) return null;

            Element role = getChild(timeSeries, "UtilitiesMeasurementTaskAssignmentRole");
            if (role != null) {
                data.setObisCode(getText(role, "UtilitiesObjectIdentificationSystemCodeText"));
                
                Element podContainer = getChild(role, "UtilitiesPointOfDeliveryExternalIdentification");
                if (podContainer != null) {
                    // Try PartyID first (Standard), fall back to MeterSerialID (Variant)
                    String podId = getText(podContainer, "UtilitiesPointOfDeliveryPartyID");
                    if (podId == null || podId.isEmpty()) {
                        podId = getText(podContainer, "UtilitiesMeterSerialID");
                    }
                    data.setPodId(podId);
                }
            }

            // 3. Extract Intervals
            List<IntervalData> intervals = new ArrayList<>();
            // Use "*" to safely find Items whether qualified or not
            NodeList items = timeSeries.getElementsByTagNameNS("*", "Item");
            
            for (int k = 0; k < items.getLength(); k++) {
                IntervalData interval = parseInterval((Element) items.item(k));
                if (interval != null) {
                    intervals.add(interval);
                }
            }
            data.setIntervals(intervals);

            return data;
        } catch (Exception e) {
            logger.error("Error parsing profile node", e);
            return null;
        }
    }

    private IntervalData parseInterval(Element item) {
        try {
            String start = getText(item, "UTCValidityStartDateTime");
            String end = getText(item, "UTCValidityEndDateTime");
            
            Element qtyEl = getChild(item, "Quantity");
            String val = (qtyEl != null) ? qtyEl.getTextContent() : null;
            String unit = (qtyEl != null) ? qtyEl.getAttribute("unitCode") : "KWH";

            String status = defaultStatus;
            Element statusWrap = getChild(item, "ItemStatus");
            if (statusWrap != null) {
                String s = getText(statusWrap, "UtilitiesTimeSeriesItemTypeCode");
                if (s != null && !s.isEmpty()) status = s;
            }

            if (start != null && end != null && val != null) {
                IntervalData id = new IntervalData();
                id.setStartDateTime(parseDateTime(start));
                id.setEndDateTime(parseDateTime(end));
                id.setValue(new BigDecimal(val.trim()));
                id.setUnitCode(unit);
                id.setStatus(status);
                return id;
            }
        } catch (Exception e) {
            logger.debug("Skipping invalid interval: {}", e.getMessage());
        }
        return null;
    }

    private String getText(Element parent, String localName) {
        NodeList nl = parent.getElementsByTagNameNS("*", localName);
        return (nl.getLength() > 0) ? nl.item(0).getTextContent().trim() : null;
    }

    private Element getChild(Element parent, String localName) {
        NodeList nl = parent.getElementsByTagNameNS("*", localName);
        return (nl.getLength() > 0) ? (Element) nl.item(0) : null;
    }

    private LocalDateTime parseDateTime(String text) {
        if (text == null) return null;
        // Handle "Z" UTC indicator
        String iso = text.endsWith("Z") ? text.substring(0, text.length() - 1) : text;
        return LocalDateTime.parse(iso);
    }
}