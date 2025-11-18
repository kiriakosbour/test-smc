package com.hedno.integration.processor;

import com.hedno.integration.dao.OrderPackageDAO;
import com.hedno.integration.dao.IntervalDataDAO;
import com.hedno.integration.entity.OrderItem;
import com.hedno.integration.entity.OrderPackage;
import com.hedno.integration.service.IntervalParsingService; // Use the new service

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REFACTORED OrderProcessor.
 * This EJB's goal is to:
 * 1. Find 'OPEN' packages from SMC_ORDER_PACKAGES.
 * 2. Get the associated 'PENDING' items from SMC_ORDER_ITEMS.
 * 3. Parse the raw XML from each item.
 * 4. Save the 96 parsed intervals into the SMC_LOAD_PROFILE_INTERVALS table.
 * 5. Mark the package and items as 'COMPLETED' / 'PROCESSED'.
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
    private IntervalDataDAO intervalDataDAO;
    private IntervalParsingService parsingService;

    // --- Config properties ---
    private int packageMaxSize;
    private int packageMaxAgeMinutes;

    @PostConstruct
    public void initialize() {
        // Load configuration from system properties or application.properties
        this.packageMaxAgeMinutes = Integer.parseInt(
            System.getProperty("order.processor.delay.minutes", "60"));
        this.packageMaxSize = Integer.parseInt(
            System.getProperty("order.processor.max.size", "100"));

        // Initialize DAOs and Services
        this.orderPackageDAO = new OrderPackageDAO();
        this.intervalDataDAO = new IntervalDataDAO();
        this.parsingService = new IntervalParsingService();

        // Start timer
        long interval = Long.parseLong(
            System.getProperty("order.processor.interval.ms", "300000")); // 5 mins
        TimerConfig timerConfig = new TimerConfig("OrderProcessorTimer", false);
        timerService.createIntervalTimer(interval, interval, timerConfig);

        logger.info("OrderProcessorEJB (Parse-and-Save) initialized. MaxPackageSize: {}, MaxAge: {} mins",
                packageMaxSize, packageMaxAgeMinutes);
    }

    /**
     * Main timer-driven method to process ready packages.
     */
    @Timeout
    public void processOrders(Timer timer) {
        logger.debug("Running OrderProcessor (Parse-and-Save) cycle...");

        // Find packages that are old enough or big enough to be processed
        List<Long> readyPackages = orderPackageDAO.findReadyPackages(packageMaxAgeMinutes, packageMaxSize);

        for (Long packageId : readyPackages) {
            try {
                // 1. Mark package as 'PROCESSING' to prevent re-processing
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.PROCESSING);

                // 2. Get all items for this package
                List<OrderItem> items = orderPackageDAO.getItemsForPackage(packageId);

                // 3. Filter for only the data type we care about
                List<OrderItem> filteredItems = items.stream()
                        .filter(item -> "Energie / Consumption".equals(item.getDataType()))
                        .collect(Collectors.toList());

                // Parse each item's XML and save its intervals to the DB
                int totalIntervalsSaved = 0;
                for (OrderItem item : filteredItems) {
                    
                    // Call the parsing service
                    List<IntervalData> intervals = parsingService.parseIntervalsFromXml(
                        item.getRawXml(), item.getProfilBlocId()
                    );
                    
                    if (!intervals.isEmpty()) {
                        // Call the new DAO to batch-insert the intervals
                        intervalDataDAO.batchInsertIntervals(item.getItemId(), intervals);
                        totalIntervalsSaved += intervals.size();
                    }
                }
                
                logger.info("Package {}: Parsed {} items and saved a total of {} intervals.", 
                    packageId, filteredItems.size(), totalIntervalsSaved);

                // 5. Mark all items in the package as 'PROCESSED'
                orderPackageDAO.updateItemStatusForPackage(packageId, OrderItem.ItemStatus.PROCESSED);
                
                // 6. Mark the parent package as 'COMPLETED'
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.COMPLETED);

            } catch (Exception e) {
                logger.error("Failed to process order package: {}", packageId, e);
                // If anything fails, mark the package as 'FAILED' for review
                orderPackageDAO.updatePackageStatus(packageId, OrderPackage.PackageStatus.FAILED);
            }
        }
    }
}