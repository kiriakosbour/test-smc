package com.hedno.integration.service;

import com.hedno.integration.processor.IntervalData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
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
 */
public class IntervalParsingService {

    private static final Logger logger = LoggerFactory.getLogger(IntervalParsingService.class);
    private DocumentBuilderFactory factory;

    public IntervalParsingService() {
        this.factory = DocumentBuilderFactory.newInstance();
    }

    /**
     * Parses the raw ProfilBloc XML and generates a list of 96 intervals.
     *
     * @param rawXml The XML from SMC_ORDER_ITEMS
     * @param profilBlocId The ID for logging
     * @return A list of IntervalData objects
     */
    public List<IntervalData> parseIntervalsFromXml(String rawXml, String profilBlocId) {
        List<IntervalData> intervals = new ArrayList<>();

        try (InputStream stream = new ByteArrayInputStream(rawXml.getBytes(StandardCharsets.UTF_8))) {
            Document doc = factory.newDocumentBuilder().parse(stream);

            String startStr = getTagValue(doc, "Start");
            String endStr = getTagValue(doc, "End");
            String valueStr = getTagValue(doc, "Value");

            if (startStr == null || endStr == null || valueStr == null) {
                logger.error("Missing required fields (Start, End, or Value) in XML for {}", profilBlocId);
                return intervals;
            }

            LocalDateTime start = LocalDateTime.parse(startStr);
            BigDecimal totalValue = new BigDecimal(valueStr);
            
            // Generate 96 intervals (15-minute intervals for a full day)
            int intervalCount = 96;
            BigDecimal intervalValue = totalValue.divide(new BigDecimal(intervalCount), 6, BigDecimal.ROUND_HALF_UP);

            for (int i = 0; i < intervalCount; i++) {
                IntervalData interval = new IntervalData();
                LocalDateTime intervalStart = start.plusMinutes(i * 15);
                LocalDateTime intervalEnd = intervalStart.plusMinutes(15).minusSeconds(1);

                interval.setStartDateTime(intervalStart);
                interval.setEndDateTime(intervalEnd);
                interval.setValue(intervalValue);
                interval.setUnitCode("KWH");
                interval.setStatus("W");
                intervals.add(interval);
            }

        } catch (Exception e) {
            logger.error("Failed to parse XML for {}", profilBlocId, e);
        }
        return intervals;
    }

    private String getTagValue(Document doc, String tag) {
        try {
            return doc.getElementsByTagName(tag).item(0).getTextContent();
        } catch (Exception e) {
            return null;
        }
    }
}