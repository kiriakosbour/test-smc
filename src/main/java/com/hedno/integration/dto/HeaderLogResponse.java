package com.hedno.integration.dto;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * DTO for MDM Header Log Response v3.0
 * 
 * Changes from v2:
 * - Renamed errorMsg to statusMsg (per architect: κενό εκτός αν πάει κάτι στραβά)
 * - Added fileId, fileName for ITRON support
 * - Added wsdlOperation tracking
 * - ProcessingSummary includes sectionUuid
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class HeaderLogResponse {
    
    // Primary key
    private long logId;
    
    // Source classification
    private String sourceSystem;      // ZFA, ITRON
    private String sourceType;        // MEASURE, ALARM, EVENT
    
    // File identification (for ITRON)
    private String fileId;
    private String fileName;
    
    // Message identification
    private String messageUuid;
    private String wsdlOperation;
    private String endpoint;
    
    // Sender/Recipient
    private String senderId;
    private String recipientId;
    
    // Timestamps
    private Timestamp sourceCreationDt;
    private Timestamp receivedAt;
    
    // Processing status
    private String status;            // PENDING, SUCCESS, PARTIAL, ERROR
    private String statusMsg;         // Empty unless error (renamed from errorMsg)
    private int recordsProcessed;
    
    // Processing summary (curve records created)
    private List<ProcessingSummary> processingSummary;
    
    // System timestamps
    private Timestamp dtCreate;
    private Timestamp dtUpdate;
    
    /**
     * Default constructor
     */
    public HeaderLogResponse() {
        this.processingSummary = new ArrayList<>();
    }
    
    /**
     * Inner class for processing summary
     * Represents each curve row created from this header
     */
    public static class ProcessingSummary {
        private String podId;
        private String supplyNum;
        private String dataClass;
        private String dateRead;
        private String sectionUuid;     // NEW: Inner message UUID per channel
        private Timestamp sourceCreationDt;
        private Timestamp dtCreate;
        
        // Getters and setters
        public String getPodId() { return podId; }
        public void setPodId(String podId) { this.podId = podId; }
        
        public String getSupplyNum() { return supplyNum; }
        public void setSupplyNum(String supplyNum) { this.supplyNum = supplyNum; }
        
        public String getDataClass() { return dataClass; }
        public void setDataClass(String dataClass) { this.dataClass = dataClass; }
        
        public String getDateRead() { return dateRead; }
        public void setDateRead(String dateRead) { this.dateRead = dateRead; }
        
        public String getSectionUuid() { return sectionUuid; }
        public void setSectionUuid(String sectionUuid) { this.sectionUuid = sectionUuid; }
        
        public Timestamp getSourceCreationDt() { return sourceCreationDt; }
        public void setSourceCreationDt(Timestamp sourceCreationDt) { 
            this.sourceCreationDt = sourceCreationDt; 
        }
        
        public Timestamp getDtCreate() { return dtCreate; }
        public void setDtCreate(Timestamp dtCreate) { this.dtCreate = dtCreate; }
        
        @Override
        public String toString() {
            return String.format("ProcessingSummary[podId=%s, dataClass=%s, dateRead=%s, sectionUuid=%s]",
                podId, dataClass, dateRead, sectionUuid);
        }
    }
    
    // ==========================================================================
    // Getters and Setters
    // ==========================================================================
    
    public long getLogId() { return logId; }
    public void setLogId(long logId) { this.logId = logId; }
    
    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }
    
    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }
    
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }
    
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    
    public String getMessageUuid() { return messageUuid; }
    public void setMessageUuid(String messageUuid) { this.messageUuid = messageUuid; }
    
    public String getWsdlOperation() { return wsdlOperation; }
    public void setWsdlOperation(String wsdlOperation) { this.wsdlOperation = wsdlOperation; }
    
    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
    
    public String getSenderId() { return senderId; }
    public void setSenderId(String senderId) { this.senderId = senderId; }
    
    public String getRecipientId() { return recipientId; }
    public void setRecipientId(String recipientId) { this.recipientId = recipientId; }
    
    public Timestamp getSourceCreationDt() { return sourceCreationDt; }
    public void setSourceCreationDt(Timestamp sourceCreationDt) { 
        this.sourceCreationDt = sourceCreationDt; 
    }
    
    public Timestamp getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Timestamp receivedAt) { this.receivedAt = receivedAt; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    /**
     * Get status message (renamed from getErrorMsg)
     * Per architect: κενό εκτός αν πάει κάτι στραβά (empty unless error)
     */
    public String getStatusMsg() { return statusMsg; }
    public void setStatusMsg(String statusMsg) { this.statusMsg = statusMsg; }
    
    public int getRecordsProcessed() { return recordsProcessed; }
    public void setRecordsProcessed(int recordsProcessed) { 
        this.recordsProcessed = recordsProcessed; 
    }
    
    public List<ProcessingSummary> getProcessingSummary() { return processingSummary; }
    public void setProcessingSummary(List<ProcessingSummary> processingSummary) { 
        this.processingSummary = processingSummary; 
    }
    
    public void addProcessingSummary(ProcessingSummary summary) {
        if (this.processingSummary == null) {
            this.processingSummary = new ArrayList<>();
        }
        this.processingSummary.add(summary);
    }
    
    public Timestamp getDtCreate() { return dtCreate; }
    public void setDtCreate(Timestamp dtCreate) { this.dtCreate = dtCreate; }
    
    public Timestamp getDtUpdate() { return dtUpdate; }
    public void setDtUpdate(Timestamp dtUpdate) { this.dtUpdate = dtUpdate; }
    
    // ==========================================================================
    // Utility Methods
    // ==========================================================================
    
    /**
     * Check if processing was successful
     */
    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }
    
    /**
     * Check if processing had errors
     */
    public boolean isError() {
        return "ERROR".equals(status);
    }
    
    /**
     * Check if processing is pending
     */
    public boolean isPending() {
        return "PENDING".equals(status);
    }
    
    @Override
    public String toString() {
        return String.format(
            "HeaderLogResponse[logId=%d, source=%s/%s, uuid=%s, status=%s, records=%d]",
            logId, sourceSystem, sourceType, messageUuid, status, recordsProcessed
        );
    }
}
