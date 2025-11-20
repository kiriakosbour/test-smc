package com.hedno.integration.service;

import com.hedno.integration.processor.IntervalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Service dedicated to parsing ProfilBloc XML into interval data.
 * UPDATED: Now parses individual <Item> elements from <TimeSeries>.
 */
public class IntervalParsingService {

    private static final Logger logger = LoggerFactory.getLogger(IntervalParsingService.class);
    private DocumentBuilderFactory factory;
    private String defaultStatus;

    public IntervalParsingService() {
        this.factory = DocumentBuilderFactory.newInstance();
        this.defaultStatus = System.getProperty("data.default.status", "W");
    }

    /**
     * Parses the raw ProfilBloc XML and generates a list of intervals found in <TimeSeries>.
     *
     * @param rawXml The XML from SMC_ORDER_ITEMS
     * @param profilBlocId The ID for logging
     * @return A list of IntervalData objects
     */
    public List<IntervalData> parseIntervalsFromXml(String rawXml, String profilBlocId) {
        List<IntervalData> intervals = new ArrayList<>();

        try (InputStream stream = new ByteArrayInputStream(rawXml.getBytes(StandardCharsets.UTF_8))) {
            Document doc = factory.newDocumentBuilder().parse(stream);

            // Get all <Item> tags anywhere in the document
            NodeList itemNodes = doc.getElementsByTagName("Item");
            
            if (itemNodes.getLength() == 0) {
                logger.warn("No <Item> tags found in XML for {}", profilBlocId);
                return intervals;
            }

            for (int i = 0; i < itemNodes.getLength(); i++) {
                Node node = itemNodes.item(i);

                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;

                    String startStr = getTagValue(element, "Start");
                    String endStr = getTagValue(element, "End");
                    String valueStr = getTagValue(element, "Value");
                    String statusStr = getTagValue(element, "Status"); // Optional

                    if (startStr != null && endStr != null && valueStr != null) {
                        try {
                            IntervalData interval = new IntervalData();
                            interval.setStartDateTime(LocalDateTime.parse(startStr));
                            interval.setEndDateTime(LocalDateTime.parse(endStr));
                            interval.setValue(new BigDecimal(valueStr));
                            interval.setStatus((statusStr != null && !statusStr.isEmpty()) ? statusStr : this.defaultStatus);
                            interval.setUnitCode("KWH"); // Default or extract from XML if available

                            intervals.add(interval);
                        } catch (Exception e) {
                            logger.error("Error parsing item #{} in {}", i, profilBlocId, e);
                        }
                    } else {
                        logger.warn("Skipping incomplete item #{} in {}", i, profilBlocId);
                    }
                }
            }

        } catch (Exception e) {
            logger.error("Failed to parse XML for {}", profilBlocId, e);
        }
        return intervals;
    }

    /**
     * Helper to get text content of a tag within a specific Element.
     */
    private String getTagValue(Element element, String tag) {
        try {
            NodeList nodeList = element.getElementsByTagName(tag);
            if (nodeList.getLength() > 0) {
                return nodeList.item(0).getTextContent();
            }
        } catch (Exception e) {
            // Ignore
        }
        return null;
    }
}