package com.hedno.integration.processor;

import com.hedno.integration.dao.LoadProfileInboundDAO;
import com.hedno.integration.dao.OrderPackageDAO;
import com.hedno.integration.entity.LoadProfileInbound;
import com.hedno.integration.entity.LoadProfileInbound.ProcessingStatus;
import com.hedno.integration.entity.OrderItem;
import com.hedno.integration.entity.OrderPackage;
import com.hedno.integration.service.XMLBuilderService;
import com.hedno.integration.service.XMLBuilderService.LoadProfileData;
import com.hedno.integration.service.XMLBuilderService.IntervalData;
import com.hedno.integration.soap.model.UtilitiesTimeSeriesERPItemBulkNotification;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.*;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * New processor to collect, batch, and prepare "order items"
 * for the LoadProfileProcessorAsync.
 */
@Singleton
@Startup
@LocalBean
@TransactionManagement(TransactionManagementType.BEAN)
public class OrderProcessor {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessor.class);

    @Resource
    private TimerService timerService;

    private OrderPackageDAO orderPackageDAO;
    private LoadProfileInboundDAO loadProfileInboundDAO; // Existing DAO
    private XMLBuilderService xmlBuilder; // Existing service

    private int maxProfilesPerXML;
    private int packageMaxSize;
    private int packageMaxAgeMinutes;

    @PostConstruct
    public void initialize() {
        // Load configuration
        this.packageMaxAgeMinutes = Integer.parseInt(System.getProperty("order.processor.delay.minutes", "60"));
        this.packageMaxSize = Integer.parseInt(System.getProperty("order.processor.max.size", "100"));

        // Use existing config for XML size
        this.maxProfilesPerXML = Integer.parseInt(
                System.getProperty("processor.max.profiles.per.message", "10"));

        // Initialize DAOs and Services
        this.orderPackageDAO = new OrderPackageDAO(); // Assumes default constructor
        this.loadProfileInboundDAO = new LoadProfileInboundDAO(); // Assumes default constructor
        try {
            this.xmlBuilder = new XMLBuilderService(maxProfilesPerXML);
        } catch (Exception e) {
            logger.error("Failed to initialize XMLBuilderService", e);
            throw new RuntimeException("Init failed", e);
        }

        // Start timer
        long interval = Long.parseLong(System.getProperty("order.processor.interval.ms", "300000")); // 5 mins
        TimerConfig timerConfig = new TimerConfig("OrderProcessorTimer", false);
        timerService.createIntervalTimer(interval, interval, timerConfig);

        logger.info("OrderProcessorEJB initialized. MaxPackageSize: {}, MaxAge: {} mins",
                packageMaxSize, packageMaxAgeMinutes);
    }

    @Timeout
    public void processOrders(Timer timer) {
        logger.debug("Running OrderProcessor cycle...");

        List<Long> readyPackages = orderPackageDAO.findReadyPackages(packageMaxAgeMinutes, packageMaxSize);

        for (Long packageId : readyPackages) {
            try {
                // 1. Mark as processing
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.PROCESSING);

                // 2. Get all items
                List<OrderItem> items = orderPackageDAO.getItemsForPackage(packageId);

                // 3. Filter for "Energie / Consumption"
                List<OrderItem> filteredItems = items.stream()
                        .filter(item -> "Energie / Consumption".equals(item.getDataType()))
                        .collect(Collectors.toList());

                // 4. Convert items to the format XMLBuilder expects
                List<LoadProfileData> allProfiles = convertItemsToLoadProfileData(filteredItems);

                // 5. Chunk the profiles into smaller lists (e.g., 100 items -> 10 lists of 10)
                // This respects the 'processor.max.profiles.per.message'
                List<List<LoadProfileData>> xmlChunks = chunkList(allProfiles, maxProfilesPerXML);

                // 6. Build XML and insert into INBOUND table
                for (List<LoadProfileData> chunk : xmlChunks) {
                    UtilitiesTimeSeriesERPItemBulkNotification notification = xmlBuilder.buildBulkNotification(chunk); //

                    String xmlPayload = xmlBuilder.marshalToXml(notification);
                    String messageUuid = notification.getMessageHeader().getUuid();

                    // Create the Inbound entity
                    LoadProfileInbound inboundMessage = new LoadProfileInbound(messageUuid, xmlPayload);
                    inboundMessage.setStatus(ProcessingStatus.PENDING); //

                    // Insert for 'LoadProfileProcessorAsync' to find
                    loadProfileInboundDAO.insert(inboundMessage); //

                    logger.info("Created new LoadProfileInbound message {} for package {}",
                            messageUuid, packageId);
                }

                // 7. Mark package as completed
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.COMPLETED);

            } catch (Exception e) {
                logger.error("Failed to process order package: {}", packageId, e);
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.FAILED);
            }
        }
    }

    /**
     * Converts your new DAO entities into the LoadProfileData class
     * required by the XMLBuilderService.
     */
    private List<LoadProfileData> convertItemsToLoadProfileData(List<OrderItem> items) {
        List<LoadProfileData> profiles = new ArrayList<>();

        for (OrderItem item : items) {
            try {
                // Parse the stored raw XML to extract interval data
                List<IntervalData> intervals = parseIntervalsFromXml(item.getRawXml(), item.getProfilBlocId());

                if (intervals.isEmpty()) {
                    logger.warn("No intervals found for item {}, skipping", item.getProfilBlocId());
                    continue;
                }

                LoadProfileData profile = new LoadProfileData();
                profile.setObisCode(item.getObisCode());
                profile.setPodId(item.getPodId());
                profile.setIntervals(intervals);

                profiles.add(profile);

            } catch (Exception e) {
                logger.error("Failed to convert item {} to profile data", item.getProfilBlocId(), e);
            }
        }

        return profiles;
    }

    /**
     * Parse intervals from the raw XML stored in the database.
     * For your test data, we'll create synthetic 15-minute intervals.
     */
    private List<IntervalData> parseIntervalsFromXml(String rawXml, String profilBlocId) {
        List<IntervalData> intervals = new ArrayList<>();

        try {
            // Parse the XML to extract Start, End, Value
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            Document doc = factory.newDocumentBuilder().parse(
                    new ByteArrayInputStream(rawXml.getBytes(StandardCharsets.UTF_8)));

            String startStr = getTagValue(doc, "Start");
            String endStr = getTagValue(doc, "End");
            String valueStr = getTagValue(doc, "Value");

            if (startStr == null || endStr == null || valueStr == null) {
                logger.error("Missing required fields in XML for {}", profilBlocId);
                return intervals;
            }

            LocalDateTime start = LocalDateTime.parse(startStr);
            LocalDateTime end = LocalDateTime.parse(endStr);
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

            logger.info("Generated {} intervals for {}", intervalCount, profilBlocId);

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

    /**
     * Utility to break a large list into chunks.
     */
    private <T> List<List<T>> chunkList(List<T> list, int chunkSize) {
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += chunkSize) {
            chunks.add(list.subList(i, Math.min(i + chunkSize, list.size())));
        }
        return chunks;
    }

}