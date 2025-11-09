package com.hedno.integration.entity;

import java.sql.Timestamp;

/**
 * Enhanced entity class representing the SMC_LOAD_PROFILE_INBOUND table.
 * Updated to support new status model: PENDING -> PROCESSING -> COMPLETED/FAILED
 * With preservation of original Message ID for manual retry.
 * * NOTE: Added attemptCount and lastAttemptTimestamp to match v1.0 DAO/DB schema.
 * * @author HEDNO Integration Team
 * @version 2.1 (Fixed)
 */
public class LoadProfileInbound {
    
    // Primary Key - UUID from the main MessageHeader
    private String messageUuid;
    
    // Complete raw XML payload for auditing and reprocessing
    private String rawPayload;
    
    // Current processing status (new model)
    private ProcessingStatus status;
    
    // Timestamps for tracking
    private Timestamp receivedTimestamp;
    private Timestamp processingStartTime;
    private Timestamp processingEndTime;
    private Timestamp lastManualRetryTime;
    private Timestamp lastAttemptTimestamp; // <-- ADDED to match DAO/DB
    
    // HTTP Response tracking (per requirement)
    private int lastHttpStatusCode;
    private String lastResponseMessage;
    private String lastErrorMessage;
    
    // Original Message ID preservation for manual retry
    private String originalMessageId;
    
    // Additional metadata
    private int manualRetryCount;
    private int attemptCount; // <-- ADDED to match DAO/DB
    private String processedBy;
    private String notes;
    
    /**
     * Enhanced enum for new processing states
     */
    public enum ProcessingStatus {
        PENDING("PENDING"),           // Awaiting processing (initial state)
        PROCESSING("PROCESSING"),      // Currently being processed
        COMPLETED("COMPLETED"),        // Successfully sent (HTTP 200)
        FAILED("FAILED");             // Failed (non-200 HTTP) - awaiting manual retry
        
        private final String value;
        
        ProcessingStatus(String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
        
        public static ProcessingStatus fromValue(String value) {
            // Handle new and old statuses during transition
            if ("RECEIVED".equals(value) || "FAILED_RETRY".equals(value)) {
                return PENDING;
            }
            if ("SENT_OK".equals(value)) {
                return COMPLETED;
            }
            if ("FAILED_DLQ".equals(value)) {
                return FAILED;
            }
            
            for (ProcessingStatus status : ProcessingStatus.values()) {
                if (status.value.equals(value)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid status: " + value);
        }
        
        /**
         * Check if status allows processing
         */
        public boolean isProcessable() {
            return this == PENDING; // Only process PENDING
        }
        
        /**
         * Check if status is terminal (no further processing needed)
         */
        public boolean isTerminal() {
            return this == COMPLETED;
        }
        
        /**
         * Check if manual retry is allowed
         */
        public boolean allowsManualRetry() {
            return this == FAILED;
        }
    }
    
    /**
     * Default constructor
     */
    public LoadProfileInbound() {
        this.status = ProcessingStatus.PENDING;
        this.receivedTimestamp = new Timestamp(System.currentTimeMillis());
        this.manualRetryCount = 0;
        this.attemptCount = 0; // <-- ADDED
        this.lastHttpStatusCode = 0;
    }
    
    /**
     * Constructor with UUID and payload
     */
    public LoadProfileInbound(String messageUuid, String rawPayload) {
        this();
        this.messageUuid = messageUuid;
        this.rawPayload = rawPayload;
        this.originalMessageId = messageUuid; // Initially same as message UUID
    }
    
    // Primary getters and setters
    
    public String getMessageUuid() {
        return messageUuid;
    }
    
    public void setMessageUuid(String messageUuid) {
        this.messageUuid = messageUuid;
    }
    
    public String getRawPayload() {
        return rawPayload;
    }
    
    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }
    
    public ProcessingStatus getStatus() {
        return status;
    }
    
    public void setStatus(ProcessingStatus status) {
        this.status = status;
    }
    
    // Timestamp getters and setters
    
    public Timestamp getReceivedTimestamp() {
        return receivedTimestamp;
    }
    
    public void setReceivedTimestamp(Timestamp receivedTimestamp) {
        this.receivedTimestamp = receivedTimestamp;
    }
    
    public Timestamp getProcessingStartTime() {
        return processingStartTime;
    }
    
    public void setProcessingStartTime(Timestamp processingStartTime) {
        this.processingStartTime = processingStartTime;
    }
    
    public Timestamp getProcessingEndTime() {
        return processingEndTime;
    }
    
    public void setProcessingEndTime(Timestamp processingEndTime) {
        this.processingEndTime = processingEndTime;
    }
    
    public Timestamp getLastManualRetryTime() {
        return lastManualRetryTime;
    }
    
    public void setLastManualRetryTime(Timestamp lastManualRetryTime) {
        this.lastManualRetryTime = lastManualRetryTime;
    }

    // --- ADDED Getters/Setters ---
    
    public Timestamp getLastAttemptTimestamp() {
        return lastAttemptTimestamp;
    }

    public void setLastAttemptTimestamp(Timestamp lastAttemptTimestamp) {
        this.lastAttemptTimestamp = lastAttemptTimestamp;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    // --- End of ADDED Getters/Setters ---

    // HTTP Response tracking getters and setters
    
    public int getLastHttpStatusCode() {
        return lastHttpStatusCode;
    }
    
    public void setLastHttpStatusCode(int lastHttpStatusCode) {
        this.lastHttpStatusCode = lastHttpStatusCode;
    }
    
    public String getLastResponseMessage() {
        return lastResponseMessage;
    }
    
    public void setLastResponseMessage(String lastResponseMessage) {
        // Truncate to fit database column if needed
        if (lastResponseMessage != null && lastResponseMessage.length() > 500) {
            this.lastResponseMessage = lastResponseMessage.substring(0, 497) + "...";
        } else {
            this.lastResponseMessage = lastResponseMessage;
        }
    }
    
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }
    
    public void setLastErrorMessage(String lastErrorMessage) {
        // Truncate to 4000 chars to fit database column
        if (lastErrorMessage != null && lastErrorMessage.length() > 4000) {
            this.lastErrorMessage = lastErrorMessage.substring(0, 3997) + "...";
        } else {
            this.lastErrorMessage = lastErrorMessage;
        }
    }
    
    // Original Message ID for retry
    
    public String getOriginalMessageId() {
        return originalMessageId;
    }
    
    public void setOriginalMessageId(String originalMessageId) {
        this.originalMessageId = originalMessageId;
    }
    
    // Additional metadata getters and setters
    
    public int getManualRetryCount() {
        return manualRetryCount;
    }
    
    public void setManualRetryCount(int manualRetryCount) {
        this.manualRetryCount = manualRetryCount;
    }
    
    public void incrementManualRetryCount() {
        this.manualRetryCount++;
        this.lastManualRetryTime = new Timestamp(System.currentTimeMillis());
    }
    
    public String getProcessedBy() {
        return processedBy;
    }
    
    public void setProcessedBy(String processedBy) {
        this.processedBy = processedBy;
    }
    
    public String getNotes() {
        return notes;
    }
    
    public void setNotes(String notes) {
        // Truncate to fit database column if needed
        if (notes != null && notes.length() > 2000) {
            this.notes = notes.substring(0, 1997) + "...";
        } else {
            this.notes = notes;
        }
    }
    
    // Business logic methods
    
    /**
     * Check if message can be processed
     */
    public boolean canProcess() {
        return status.isProcessable();
    }
    
    /**
     * Check if manual retry is allowed
     */
    public boolean canManualRetry() {
        return status.allowsManualRetry();
    }
    
    /**
     * Get processing duration in milliseconds
     */
    public long getProcessingDurationMs() {
        if (processingStartTime != null && processingEndTime != null) {
            return processingEndTime.getTime() - processingStartTime.getTime();
        }
        return -1;
    }
    
    /**
     * Get age of message in hours
     */
    public long getAgeInHours() {
        if (receivedTimestamp != null) {
            long ageMs = System.currentTimeMillis() - receivedTimestamp.getTime();
            return ageMs / (1000 * 60 * 60);
        }
        return -1;
    }
    
    /**
     * Check if message is older than specified hours
     */
    public boolean isOlderThan(int hours) {
        return getAgeInHours() > hours;
    }
    
    /**
     * Mark as processing
     */
    public void markAsProcessing() {
        this.status = ProcessingStatus.PROCESSING;
        this.processingStartTime = new Timestamp(System.currentTimeMillis());
        this.processingEndTime = null;
        this.lastAttemptTimestamp = new Timestamp(System.currentTimeMillis()); // Update this
        this.attemptCount++; // Update this
    }
    
    /**
     * Mark as completed
     */
    public void markAsCompleted(int httpStatusCode, String responseMessage) {
        this.status = ProcessingStatus.COMPLETED;
        this.processingEndTime = new Timestamp(System.currentTimeMillis());
        this.lastHttpStatusCode = httpStatusCode;
        this.lastResponseMessage = responseMessage;
        this.lastErrorMessage = null;
    }
    
    /**
     * Mark as failed
     */
    public void markAsFailed(int httpStatusCode, String responseMessage, String errorMessage) {
        this.status = ProcessingStatus.FAILED;
        this.processingEndTime = new Timestamp(System.currentTimeMillis());
        this.lastHttpStatusCode = httpStatusCode;
        this.lastResponseMessage = responseMessage;
        this.lastErrorMessage = errorMessage;
        
        // Preserve original message ID for retry
        if (this.originalMessageId == null) {
            this.originalMessageId = this.messageUuid;
        }
    }
    
    /**
     * Reset for manual retry
     */
    public void resetForManualRetry() {
        this.status = ProcessingStatus.PENDING;
        this.processingStartTime = null;
        this.processingEndTime = null;
        this.lastHttpStatusCode = 0;
        this.lastResponseMessage = null;
        this.lastErrorMessage = null;
        this.attemptCount = 0; // Reset attempt count
        this.incrementManualRetryCount();
    }
    
    @Override
    public String toString() {
        return String.format(
            "LoadProfileInbound{uuid='%s', status=%s, httpCode=%d, " +
            "received=%s, originalId='%s', retries=%d, age=%dh}",
            messageUuid, status, lastHttpStatusCode, receivedTimestamp,
            originalMessageId, manualRetryCount, getAgeInHours()
        );
    }
    
    /**
     * Create a summary for logging
     */
    public String toLogSummary() {
        return String.format(
            "Message[%s] Status:%s HTTP:%d Duration:%dms Retries:%d Age:%dh",
            messageUuid, status.getValue(), lastHttpStatusCode,
            getProcessingDurationMs(), manualRetryCount, getAgeInHours()
        );
    }
}