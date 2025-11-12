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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


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
    private LoadProfileInboundDAO loadProfileInboundDAO;
    private XMLBuilderService xmlBuilder; 
    
    private int maxProfilesPerXML;
    private int packageMaxSize;
    private int packageMaxAgeMinutes;

    @PostConstruct
    public void initialize() {
        // Load configuration
        this.packageMaxAgeMinutes = Integer.parseInt(System.getProperty("order.processor.delay.minutes", "0"));
        this.packageMaxSize = Integer.parseInt(System.getProperty("order.processor.max.size", "1"));

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
        long interval = Long.parseLong(System.getProperty("order.processor.interval.ms", "3000")); // 5 mins
        TimerConfig timerConfig = new TimerConfig("OrderProcessorTimer", false);
        timerService.createIntervalTimer(interval, interval, timerConfig);
        
        logger.info("OrderProcessorEJB initialized. MaxPackageSize: {}, MaxAge: {} mins", 
            packageMaxSize, packageMaxAgeMinutes);
    }

    @Timeout
    public void processOrders(Timer timer) {
        logger.info("========================================");
        logger.info("🔁 OrderProcessor tick triggered at {}", new java.util.Date());
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
                    UtilitiesTimeSeriesERPItemBulkNotification notification =
                        xmlBuilder.buildBulkNotification(chunk); //

                    String xmlPayload = xmlBuilder.marshalToXml(notification);
                    xmlPayload = xmlPayload.replaceFirst("(?s)^\\s*<\\?xml[^>]*\\?>\\s*", "");
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
            LoadProfileData profile = new LoadProfileData();
            profile.setObisCode(item.getObisCode());
            profile.setPodId(item.getPodId());

            List<IntervalData> intervals = new ArrayList<>();
            try {
                intervals = orderPackageDAO.fetchIntervalDataForBloc(item.getProfilBlocId());
            } catch (Exception e) {
                logger.error("Failed to load interval data for bloc {}", item.getProfilBlocId(), e);
            }

            profile.setIntervals(intervals);
            profiles.add(profile);
        }

        return profiles;
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
    
    // Define a simple placeholder class
    private static class OrderPackageItem {
        String getDataType() { return "Energie / Consumption"; }
        String getObisCode() { return "1.29.99.128"; }
        String getPodId() { return "HU000130F110S-TEST-001"; }
    }
}