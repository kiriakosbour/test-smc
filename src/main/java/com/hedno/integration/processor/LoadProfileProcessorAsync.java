package com.hedno.integration.processor;

import com.hedno.integration.dao.LoadProfileInboundDAO;
import com.hedno.integration.entity.LoadProfileInbound;
import com.hedno.integration.entity.LoadProfileInbound.ProcessingStatus;
import com.hedno.integration.service.SimpleXMLBuilderService;
import com.hedno.integration.service.SoapClientServiceAsync;
import com.hedno.integration.service.SoapClientServiceAsync.SoapResponse;
import com.hedno.integration.service.SoapClientServiceAsync.SoapResponseCallback;
import com.hedno.integration.service.SoapClientServiceUpdated;
import com.hedno.integration.service.XMLBuilderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hedno.integration.service.SimpleXMLBuilderService;
import com.hedno.integration.service.SoapClientServiceUpdated;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.*;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enhanced processor that handles asynchronous SOAP communication.
 * Implements new status model: COMPLETED/FAILED with no automatic retry.
 * 
 * Key Changes:
 * - Asynchronous SOAP calls with callbacks
 * - Preserves original Message ID for manual retry
 * - Improved concurrent processing
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
@Singleton
@Startup
@LocalBean
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
@TransactionManagement(TransactionManagementType.BEAN)
public class LoadProfileProcessorAsync {

    private static final Logger logger = LoggerFactory.getLogger(LoadProfileProcessorAsync.class);

    // Configuration constants
    private static final String TIMER_NAME = "LoadProfileProcessorTimer";
    private static final long DEFAULT_PROCESSING_INTERVAL_MS = 60000; // 1 minute
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final int DEFAULT_MAX_CONCURRENT = 5;
    private static final int DEFAULT_MAX_PROFILES_PER_MESSAGE = 10;
    private static final long ASYNC_TIMEOUT_MS = 120000;
    private static final int DEFAULT_MAX_RETRIES = 5;
    // Services
    private LoadProfileInboundDAO dao;


    // Configuration
    private long processingIntervalMs;
    private int batchSize;
    private int maxConcurrent;
    private int maxProfilesPerMessage;
    private int maxRetryAttempts;

    // Processing state
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicInteger activeAsyncRequests = new AtomicInteger(0);
    private final Map<String, ProcessingContext> activeContexts = new ConcurrentHashMap<>();
    private Timer processingTimer;

    // Statistics
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private final AtomicInteger totalCompleted = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);

    @Resource
    private TimerService timerService;

    /**
     * Processing context for tracking async operations
     */
    private static class ProcessingContext {
        final String messageUuid;
        final long startTime;
        final CountDownLatch latch;
        volatile boolean completed;
        volatile int httpStatusCode;

        ProcessingContext(String messageUuid) {
            this.messageUuid = messageUuid;
            this.startTime = System.currentTimeMillis();
            this.latch = new CountDownLatch(1);
            this.completed = false;
            this.httpStatusCode = -1;
        }
    }
private SimpleXMLBuilderService xmlBuilder;
private SoapClientServiceUpdated soapClient;
    /**
     * Initialize the processor on startup
     */
@PostConstruct
public void initialize() {
    // Initialize with new services
    xmlBuilder = new SimpleXMLBuilderService();
    
    String endpointUrl = System.getProperty("endpoint.url");
    String username = System.getProperty("endpoint.username", "");
    String password = System.getProperty("endpoint.password", "");
    
    soapClient = new SoapClientServiceUpdated(endpointUrl, username, password);
}

    /**
     * Cleanup on shutdown
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down Async LoadProfileProcessor...");

        // Cancel timer
        if (processingTimer != null) {
            processingTimer.cancel();
        }

        // Wait for current processing to complete
        int waitCount = 0;
        while ((isProcessing.get() || activeAsyncRequests.get() > 0) && waitCount < 60) {
            try {
                logger.info("Waiting for {} async requests to complete...", activeAsyncRequests.get());
                Thread.sleep(1000);
                waitCount++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Shutdown SOAP client
        if (soapClient != null) {
            soapClient.shutdown(30);
        }

        // Close DAO resources
        if (dao != null) {
            dao.close();
        }

        logger.info("LoadProfileProcessor shutdown complete. Stats - Total: {}, Completed: {}, Failed: {}",
                totalProcessed.get(), totalCompleted.get(), totalFailed.get());
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

        maxConcurrent = Integer.parseInt(
                System.getProperty("processor.max.concurrent",
                        String.valueOf(DEFAULT_MAX_CONCURRENT)));

        maxProfilesPerMessage = Integer.parseInt(
                System.getProperty("processor.max.profiles.per.message",
                        String.valueOf(DEFAULT_MAX_PROFILES_PER_MESSAGE)));
        maxRetryAttempts = Integer.parseInt(
                System.getProperty("processor.max.retries",
                        String.valueOf(DEFAULT_MAX_RETRIES)));

        logger.info("Configuration loaded - ... Max Retries: {}",
                maxRetryAttempts);

        logger.info("Configuration loaded - Interval: {}ms, Batch: {}, Max Concurrent: {}, Max Profiles/Msg: {}",
                processingIntervalMs, batchSize, maxConcurrent, maxProfilesPerMessage);
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
     * Main processing logic with async operations
     */
    private void doProcessMessages() {
        logger.debug("Starting async message processing cycle...");

        try {
            // Check if we have capacity for new async requests
            if (activeAsyncRequests.get() >= maxConcurrent) {
                logger.info("Max concurrent requests reached ({}), skipping this cycle",
                        activeAsyncRequests.get());
                return;
            }

            // Calculate how many messages we can process
            int availableSlots = Math.min(batchSize, maxConcurrent - activeAsyncRequests.get());

            // Fetch messages ready for processing (PENDING status only)
            List<LoadProfileInbound> messages = dao.getMessagesForProcessing(availableSlots);

            if (messages.isEmpty()) {
                logger.debug("No messages to process");
                return;
            }

            logger.info("Processing {} messages asynchronously", messages.size());

            // Process each message asynchronously
            CountDownLatch batchLatch = new CountDownLatch(messages.size());

            for (LoadProfileInbound message : messages) {
                processMessageAsync(message, batchLatch);
            }

            // Wait for batch completion with timeout
            boolean completed = batchLatch.await(ASYNC_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            if (!completed) {
                logger.warn("Batch processing timeout - some messages may still be processing");
            }

            logger.info("Processing cycle complete - Active async requests: {}",
                    activeAsyncRequests.get());

        } catch (Exception e) {
            logger.error("Error in processing cycle", e);
        }
    }

    /**
     * Process individual message asynchronously
     */
    private void processMessageAsync(LoadProfileInbound message, CountDownLatch batchLatch) {
        String messageUuid = message.getMessageUuid();
        logger.info("Starting async processing for message: {}", messageUuid);

        // Create processing context
        ProcessingContext context = new ProcessingContext(messageUuid);
        activeContexts.put(messageUuid, context);
        activeAsyncRequests.incrementAndGet();

        // Update status to PROCESSING
        message.setStatus(ProcessingStatus.PROCESSING);
        message.setProcessingStartTime(new Timestamp(System.currentTimeMillis()));
        dao.updateStatus(message);

        try {
            // Validate XML
            if (!xmlBuilder.validateXml(message.getRawPayload())) {
                throw new IllegalArgumentException("XML validation failed");
            }

            // Extract UUID from XML (should match database UUID)
            String xmlUuid = xmlBuilder.extractMessageUuid(message.getRawPayload());
            if (!messageUuid.equals(xmlUuid)) {
                logger.warn("UUID mismatch - DB: {}, XML: {} - Using DB UUID",
                        messageUuid, xmlUuid);
            }

            // Send asynchronously with callback
            soapClient.sendSoapMessageAsync(
                    message.getRawPayload(),
                    messageUuid,
                    new SoapResponseCallback() {
                        @Override
                        public void onSuccess(SoapResponse response) {
                            handleAsyncSuccess(message, response, context, batchLatch);
                        }

                        @Override
                        public void onFailure(SoapResponse response) {
                            handleAsyncFailure(message, response, context, batchLatch);
                        }

                        @Override
                        public void onException(String messageId, Exception e) {
                            handleAsyncException(message, e, context, batchLatch);
                        }
                    });

        } catch (Exception e) {
            // Synchronous error during setup
            logger.error("Error setting up async processing for message {}", messageUuid, e);
            handleAsyncException(message, e, context, batchLatch);
        }
    }

    /**
     * Handle successful async response (HTTP 200)
     */
    private void handleAsyncSuccess(LoadProfileInbound message, SoapResponse response,
            ProcessingContext context, CountDownLatch batchLatch) {
        try {
            logger.info("Message {} completed successfully - HTTP {} ({}ms)",
                    message.getMessageUuid(), response.getStatusCode(), response.getResponseTimeMs());

            // Update to COMPLETED status
            message.setStatus(ProcessingStatus.COMPLETED);
            message.setLastHttpStatusCode(response.getStatusCode());
            message.setLastResponseMessage(response.getStatusMessage());
            message.setProcessingEndTime(new Timestamp(System.currentTimeMillis()));
            message.setLastErrorMessage(null);
            dao.updateStatus(message);

            // Update statistics
            totalProcessed.incrementAndGet();
            totalCompleted.incrementAndGet();

            // Update context
            context.completed = true;
            context.httpStatusCode = response.getStatusCode();

        } finally {
            cleanupAsyncRequest(context, batchLatch);
        }
    }

    // Inside LoadProfileProcessorAsync.java

    /**
     * Handle failed async response (non-200 HTTP status)
     */
    private void handleAsyncFailure(LoadProfileInbound message, SoapResponse response,
            ProcessingContext context, CountDownLatch batchLatch) {
        try {
            String errorMsg = String.format("HTTP %d: %s",
                    response.getStatusCode(), response.getStatusMessage());

            // Check if we have retries left
            if (message.getAttemptCount() < maxRetryAttempts) {
                // --- RETRY LOGIC ---
                logger.warn("Message {} failed on attempt {}/{}. Retrying... Error: {}",
                        message.getMessageUuid(), message.getAttemptCount(), maxRetryAttempts, errorMsg);

                // Set status back to PENDING to be picked up again
                message.setStatus(ProcessingStatus.PENDING);
                message.setLastErrorMessage(errorMsg); // Log the error
                message.setProcessingEndTime(new Timestamp(System.currentTimeMillis())); // End this attempt

            } else {
                // --- FAILED (DEAD-LETTER) LOGIC ---
                logger.error("Message {} failed on final attempt {}/{}. Moving to FAILED. Error: {}",
                        message.getMessageUuid(), message.getAttemptCount(), maxRetryAttempts, errorMsg);

                message.setStatus(ProcessingStatus.FAILED);
                message.setLastHttpStatusCode(response.getStatusCode());
                message.setLastResponseMessage(response.getStatusMessage());
                message.setProcessingEndTime(new Timestamp(System.currentTimeMillis()));
                message.setLastErrorMessage(errorMsg);
                message.setOriginalMessageId(message.getMessageUuid());

                // Update statistics
                totalProcessed.incrementAndGet();
                totalFailed.incrementAndGet();
            }

            // Save the updated status (either PENDING or FAILED)
            dao.updateStatus(message);

            // Update context
            context.completed = false;
            context.httpStatusCode = response.getStatusCode();

        } finally {
            cleanupAsyncRequest(context, batchLatch);
        }
    }

    /**
     * Handle async exception
     */
    private void handleAsyncException(LoadProfileInbound message, Exception e,
            ProcessingContext context, CountDownLatch batchLatch) {
        try {
            logger.error("Message {} encountered exception - Preserving for manual retry",
                    message.getMessageUuid(), e);

            // Update to FAILED status
            message.setStatus(ProcessingStatus.FAILED);
            message.setLastHttpStatusCode(-1);
            message.setLastResponseMessage("Connection Error");
            message.setProcessingEndTime(new Timestamp(System.currentTimeMillis()));
            message.setLastErrorMessage("Exception: " + e.getMessage());

            // Preserve original Message ID for manual retry
            message.setOriginalMessageId(message.getMessageUuid());
            dao.updateStatus(message);

            // Update statistics
            totalProcessed.incrementAndGet();
            totalFailed.incrementAndGet();

            // Update context
            context.completed = false;
            context.httpStatusCode = -1;

        } finally {
            cleanupAsyncRequest(context, batchLatch);
        }
    }

    /**
     * Cleanup after async request completion
     */
    private void cleanupAsyncRequest(ProcessingContext context, CountDownLatch batchLatch) {
        activeContexts.remove(context.messageUuid);
        activeAsyncRequests.decrementAndGet();
        context.latch.countDown();
        batchLatch.countDown();

        long duration = System.currentTimeMillis() - context.startTime;
        logger.debug("Async request completed for {} in {}ms", context.messageUuid, duration);
    }

    /**
     * Manually retry a failed message
     * 
     * @param messageUuid The message UUID
     * @return true if resubmitted successfully
     */
    public boolean manualRetry(String messageUuid) {
        logger.info("Manual retry requested for message: {}", messageUuid);

        LoadProfileInbound message = dao.findByUuid(messageUuid);
        if (message == null) {
            logger.error("Message not found: {}", messageUuid);
            return false;
        }

        // Check if message is eligible for retry
        if (message.getStatus() != ProcessingStatus.FAILED) {
            logger.warn("Message {} is not in FAILED status, current status: {}",
                    messageUuid, message.getStatus());
            return false;
        }

        // Reset to PENDING for reprocessing
        message.setStatus(ProcessingStatus.PENDING);
        message.setProcessingStartTime(null);
        message.setProcessingEndTime(null);
        message.setLastHttpStatusCode(0);
        message.setLastResponseMessage(null);
        message.setLastErrorMessage(null);

        // Preserve original message ID
        if (message.getOriginalMessageId() == null) {
            message.setOriginalMessageId(messageUuid);
        }

        if (dao.updateStatus(message)) {
            logger.info("Message {} reset for manual retry", messageUuid);
            return true;
        }

        return false;
    }

    /**
     * Get processing statistics
     */
    public ProcessingStatistics getStatistics() {
        ProcessingStatistics stats = new ProcessingStatistics();

        stats.setPendingCount(dao.findByStatus(ProcessingStatus.PENDING).size());
        stats.setProcessingCount(dao.findByStatus(ProcessingStatus.PROCESSING).size());
        stats.setCompletedCount(dao.findByStatus(ProcessingStatus.COMPLETED).size());
        stats.setFailedCount(dao.findByStatus(ProcessingStatus.FAILED).size());
        stats.setIsProcessing(isProcessing.get());
        stats.setActiveAsyncRequests(activeAsyncRequests.get());
        stats.setTotalProcessed(totalProcessed.get());
        stats.setTotalCompleted(totalCompleted.get());
        stats.setTotalFailed(totalFailed.get());
        stats.setThreadPoolStats(soapClient.getThreadPoolStats());

        return stats;
    }

    /**
     * Enhanced statistics class
     */
    public static class ProcessingStatistics {
        private int pendingCount;
        private int processingCount;
        private int completedCount;
        private int failedCount;
        private boolean isProcessing;
        private int activeAsyncRequests;
        private int totalProcessed;
        private int totalCompleted;
        private int totalFailed;
        private String threadPoolStats;

        // Getters and setters
        public int getPendingCount() {
            return pendingCount;
        }

        public void setPendingCount(int pendingCount) {
            this.pendingCount = pendingCount;
        }

        public int getProcessingCount() {
            return processingCount;
        }

        public void setProcessingCount(int processingCount) {
            this.processingCount = processingCount;
        }

        public int getCompletedCount() {
            return completedCount;
        }

        public void setCompletedCount(int completedCount) {
            this.completedCount = completedCount;
        }

        public int getFailedCount() {
            return failedCount;
        }

        public void setFailedCount(int failedCount) {
            this.failedCount = failedCount;
        }

        public boolean isProcessing() {
            return isProcessing;
        }

        public void setIsProcessing(boolean isProcessing) {
            this.isProcessing = isProcessing;
        }

        public int getActiveAsyncRequests() {
            return activeAsyncRequests;
        }

        public void setActiveAsyncRequests(int activeAsyncRequests) {
            this.activeAsyncRequests = activeAsyncRequests;
        }

        public int getTotalProcessed() {
            return totalProcessed;
        }

        public void setTotalProcessed(int totalProcessed) {
            this.totalProcessed = totalProcessed;
        }

        public int getTotalCompleted() {
            return totalCompleted;
        }

        public void setTotalCompleted(int totalCompleted) {
            this.totalCompleted = totalCompleted;
        }

        public int getTotalFailed() {
            return totalFailed;
        }

        public void setTotalFailed(int totalFailed) {
            this.totalFailed = totalFailed;
        }

        public String getThreadPoolStats() {
            return threadPoolStats;
        }

        public void setThreadPoolStats(String threadPoolStats) {
            this.threadPoolStats = threadPoolStats;
        }

        @Override
        public String toString() {
            return String.format(
                    "Statistics[Pending=%d, Processing=%d, Completed=%d, Failed=%d, " +
                            "Active=%s, AsyncReqs=%d, Total=%d, ThreadPool=%s]",
                    pendingCount, processingCount, completedCount, failedCount,
                    isProcessing, activeAsyncRequests, totalProcessed, threadPoolStats);
        }
    }
}