package com.hedno.integration.processor;

import com.hedno.integration.dao.LoadProfileInboundDAO;
import com.hedno.integration.entity.LoadProfileInbound;
import com.hedno.integration.entity.LoadProfileInbound.ProcessingStatus;
import com.hedno.integration.service.SoapClientService;
import com.hedno.integration.service.SoapClientService.SoapResponse;
import com.hedno.integration.service.XMLBuilderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Resource;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main processor that handles the complete workflow:
 * 1. Fetch messages from database (PENDING status)
 * 2. Process and validate XML
 * 3. Send to SAP (no automatic retry)
 * 4. Update status in database
 *
 * This is implemented as a Singleton EJB with timer-based processing
 *
 * @author HEDNO Integration Team
 * @version 1.0 (Refactored for v2.1 Entity)
 */
@Singleton
@Startup
@LocalBean
@TransactionManagement(TransactionManagementType.BEAN)
public class LoadProfileProcessor {

    private static final Logger logger = LoggerFactory.getLogger(LoadProfileProcessor.class);

    // Configuration constants
    private static final String TIMER_NAME = "LoadProfileProcessorTimer";
    private static final long DEFAULT_PROCESSING_INTERVAL_MS = 60000; // 1 minute
    private static final int DEFAULT_BATCH_SIZE = 10;
    // <-- FIX: maxRetries is no longer used by the v2.1 entity, but kept for config compatibility
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_PROFILES_PER_MESSAGE = 10;

    // Services
    private LoadProfileInboundDAO dao;
    private XMLBuilderService xmlBuilder;
    private SoapClientService soapClient;
    private LoadProfileDataExtractor dataExtractor;

    // Configuration
    private long processingIntervalMs;
    private int batchSize;
    private int maxRetries;
    private int maxProfilesPerMessage;

    // Processing state
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private Timer processingTimer;

    @Resource
    private TimerService timerService;

    /**
     * Initialize the processor on startup
     */
    @PostConstruct
    public void initialize() {
        logger.info("Initializing LoadProfileProcessor...");

        try {
            // Load configuration
            loadConfiguration();

            // Initialize DAO
            dao = new LoadProfileInboundDAO();

            // Initialize XML Builder
            xmlBuilder = new XMLBuilderService(maxProfilesPerMessage);

            // Initialize SOAP Client
            String host = System.getProperty("sap.endpoint.host", "localhost");
            int port = Integer.parseInt(System.getProperty("sap.endpoint.port", "56700"));
            String path = System.getProperty("sap.endpoint.path",
                "/XISOAPAdapter/MessageServlet");
            String username = System.getProperty("sap.username");
            String password = System.getProperty("sap.password");
            boolean useHttps = Boolean.parseBoolean(
                System.getProperty("sap.use.https", "true"));

            soapClient = new SoapClientService(host, port, path, username, password, useHttps);

            // Initialize data extractor
            dataExtractor = new LoadProfileDataExtractor();

            // Test SAP connection
            if (soapClient.testConnection()) {
                logger.info("SAP endpoint connection test successful");
            } else {
                logger.warn("SAP endpoint connection test failed - will retry during processing");
            }

            // Start timer for periodic processing
            startProcessingTimer();

            logger.info("LoadProfileProcessor initialized successfully");

        } catch (Exception e) {
            logger.error("Failed to initialize LoadProfileProcessor", e);
            throw new RuntimeException("Initialization failed", e);
        }
    }

    /**
     * Cleanup on shutdown
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down LoadProfileProcessor...");

        // Cancel timer
        if (processingTimer != null) {
            processingTimer.cancel();
        }

        // Wait for current processing to complete
        int waitCount = 0;
        while (isProcessing.get() && waitCount < 30) {
            try {
                Thread.sleep(1000);
                waitCount++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Close resources
        if (dao != null) {
            dao.close();
        }

        logger.info("LoadProfileProcessor shutdown complete");
    }

    /**
     * Load configuration from system properties
     */
    private void loadConfiguration() {
        processingIntervalMs = Long.parseLong(
            System.getProperty("processor.interval.ms",
                String.valueOf(DEFAULT_PROCESSING_INTERVAL_MS)));

        batchSize = Integer.parseInt(
            System.getProperty("processor.batch.size",
                String.valueOf(DEFAULT_BATCH_SIZE)));

        maxRetries = Integer.parseInt(
            System.getProperty("processor.max.retries",
                String.valueOf(DEFAULT_MAX_RETRIES)));

        maxProfilesPerMessage = Integer.parseInt(
            System.getProperty("processor.max.profiles.per.message",
                String.valueOf(DEFAULT_MAX_PROFILES_PER_MESSAGE)));

        logger.info("Configuration loaded - Interval: {}ms, Batch: {}, Max Retries: {}, Max Profiles/Msg: {}",
            processingIntervalMs, batchSize, maxRetries, maxProfilesPerMessage);
    }

    /**
     * Start the timer for periodic processing
     */
    private void startProcessingTimer() {
        // Cancel existing timer if present
        for (Timer timer : timerService.getTimers()) {
            if (TIMER_NAME.equals(timer.getInfo())) {
                timer.cancel();
            }
        }

        // Create new timer
        TimerConfig timerConfig = new TimerConfig(TIMER_NAME, false);
        processingTimer = timerService.createIntervalTimer(
            processingIntervalMs, processingIntervalMs, timerConfig);

        logger.info("Processing timer started with interval: {} ms", processingIntervalMs);
    }

    /**
     * Timer callback for periodic processing
     */
    @Timeout
    public void processMessages(Timer timer) {
        if (!TIMER_NAME.equals(timer.getInfo())) {
            return;
        }

        // Check if already processing
        if (!isProcessing.compareAndSet(false, true)) {
            logger.debug("Processing already in progress, skipping this cycle");
            return;
        }

        try {
            doProcessMessages();
        } finally {
            isProcessing.set(false);
        }
    }

    /**
     * Main processing logic
     */
    private void doProcessMessages() {
        logger.debug("Starting message processing cycle...");

        try {
            // Fetch messages ready for processing
            // <-- FIX: Changed call to match LoadProfileInboundDAO (only takes batchSize)
            List<LoadProfileInbound> messages = dao.getMessagesForProcessing(batchSize);

            if (messages.isEmpty()) {
                logger.debug("No messages to process");
                return;
            }

            logger.info("Processing {} messages", messages.size());

            // Process each message
            int successCount = 0;
            int failureCount = 0;

            for (LoadProfileInbound message : messages) {
                try {
                    if (processMessage(message)) {
                        successCount++;
                    } else {
                        failureCount++;
                    }
                } catch (Exception e) {
                    logger.error("Error processing message {}", message.getMessageUuid(), e);
                    failureCount++;
                }
            }

            logger.info("Processing cycle complete - Success: {}, Failures: {}",
                successCount, failureCount);

        } catch (Exception e) {
            logger.error("Error in processing cycle", e);
        }
    }

    /**
     * Process individual message
     * @param message The message to process
     * @return true if successful, false otherwise
     */
    private boolean processMessage(LoadProfileInbound message) {
        logger.info("Processing message: {}", message.getMessageUuid());

        // <-- FIX: Use the entity's helper method.
        // This replaces setStatus(PROCESSING) and the non-existent incrementAttempt()
        message.markAsProcessing();
        dao.updateStatus(message);

        try {
            // Validate XML
            if (!xmlBuilder.validateXml(message.getRawPayload())) {
                throw new Exception("XML validation failed");
            }

            // Extract UUID from XML (should match database UUID)
            String xmlUuid = xmlBuilder.extractMessageUuid(message.getRawPayload());
            if (!message.getMessageUuid().equals(xmlUuid)) {
                logger.warn("UUID mismatch - DB: {}, XML: {}",
                    message.getMessageUuid(), xmlUuid);
            }

            // Send to SAP
            SoapResponse response = soapClient.sendSoapMessage(
                message.getRawPayload(), message.getMessageUuid());

            // Handle response
            if (response.isSuccess()) {
                // Success - update status
                // <-- FIX: Changed from SENT_OK to COMPLETED
                message.markAsCompleted(response.getStatusCode(), response.getStatusMessage());
                dao.updateStatus(message);

                logger.info("Message {} sent successfully", message.getMessageUuid());
                return true;

            } else {
                // Failure - no automatic retry
                String errorMsg = String.format("HTTP %d: %s",
                    response.getStatusCode(), response.getStatusMessage());
                
                // <-- FIX: Removed all 'shouldRetry' and 'FAILED_RETRY' logic.
                // The new entity (v2.1) logic moves directly to FAILED for manual retry.
                message.markAsFailed(response.getStatusCode(), response.getStatusMessage(), errorMsg);
                dao.updateStatus(message);
                logger.error("Message {} failed and moved to FAILED status. Error: {}",
                    message.getMessageUuid(), errorMsg);
                return false;
            }

        } catch (Exception e) {
            // Processing error
            String errorMsg = "Processing error: " + e.getMessage();

            // <-- FIX: Removed all 'shouldRetry' and 'FAILED_RETRY' logic.
            message.markAsFailed(500, "Internal Processing Error", errorMsg);
            dao.updateStatus(message);
            logger.error("Message {} failed with processing error, moved to FAILED status.",
                message.getMessageUuid(), e);
            return false;
        }
    }

    /**
     * Manually trigger processing (for testing or admin use)
     * @return Number of messages processed
     */
    public int triggerManualProcessing() {
        logger.info("Manual processing triggered");

        if (!isProcessing.compareAndSet(false, true)) {
            logger.warn("Processing already in progress");
            return 0;
        }

        try {
            // <-- FIX: Changed call to match LoadProfileInboundDAO
            List<LoadProfileInbound> messages = dao.getMessagesForProcessing(batchSize);

            int processed = 0;
            for (LoadProfileInbound message : messages) {
                if (processMessage(message)) {
                    processed++;
                }
            }

            logger.info("Manual processing complete: {} messages processed", processed);
            return processed;

        } finally {
            isProcessing.set(false);
        }
    }

    /**
     * Resubmit a specific message by UUID
     * @param messageUuid The message UUID
     * @return true if resubmitted successfully
     */
    public boolean resubmitMessage(String messageUuid) {
        logger.info("Resubmitting message: {}", messageUuid);

        LoadProfileInbound message = dao.findByUuid(messageUuid);
        if (message == null) {
            logger.error("Message not found: {}", messageUuid);
            return false;
        }

        // Reset for retry
        // <-- FIX: Use the entity's helper method, which changes status to PENDING
        message.resetForManualRetry();

        if (dao.updateStatus(message)) {
            logger.info("Message {} reset for reprocessing", messageUuid);
            return true;
        }

        return false;
    }

    /**
     * Get processing statistics
     */
    public ProcessingStatistics getStatistics() {
        // <-- FIX: Updated class name
        ProcessingStatistics stats = new ProcessingStatistics();

        // <-- FIX: Updated to new status model
        stats.setPendingCount(dao.findByStatus(ProcessingStatus.PENDING).size());
        stats.setProcessingCount(dao.findByStatus(ProcessingStatus.PROCESSING).size());
        stats.setCompletedCount(dao.findByStatus(ProcessingStatus.COMPLETED).size());
        stats.setFailedCount(dao.findByStatus(ProcessingStatus.FAILED).size());
        stats.setIsProcessing(isProcessing.get());

        return stats;
    }

    /**
     * Statistics class
     * <-- FIX: Renamed fields and methods to match new status model
     */
    public static class ProcessingStatistics {
        private int pendingCount;
        private int processingCount;
        private int completedCount;
        private int failedCount;
        private boolean isProcessing;

        // Getters and setters
        public int getPendingCount() { return pendingCount; }
        public void setPendingCount(int pendingCount) { this.pendingCount = pendingCount; }

        public int getProcessingCount() { return processingCount; }
        public void setProcessingCount(int processingCount) { this.processingCount = processingCount; }

        public int getCompletedCount() { return completedCount; }
        public void setCompletedCount(int completedCount) { this.completedCount = completedCount; }

        public int getFailedCount() { return failedCount; }
        public void setFailedCount(int failedCount) { this.failedCount = failedCount; }

        public boolean isProcessing() { return isProcessing; }
        public void setIsProcessing(boolean isProcessing) { this.isProcessing = isProcessing; }

        @Override
        public String toString() {
            return String.format("Statistics[Pending=%d, Processing=%d, Completed=%d, Failed=%d, Active=%s]",
                pendingCount, processingCount, completedCount, failedCount, isProcessing);
        }
    }
}