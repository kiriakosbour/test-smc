package com.hedno.integration.processor;

import java.util.List;

/**
 * Data Transfer Object for a single load profile (channel).
 * 
 * v3.0 Changes:
 * - Added messageUuid field for SECTION_UUID tracking (inner XML UUID per channel)
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class LoadProfileData {

    private String obisCode;          // OBIS code (e.g., 1-11:1.5.0)
    private String podId;             // Full 22-char Point of Delivery ID
    private String messageUuid;       // Inner message UUID (for SECTION_UUID) - NEW in v3
    private List<IntervalData> intervals;

    // Getters and Setters

    public String getObisCode() {
        return obisCode;
    }

    public void setObisCode(String obisCode) {
        this.obisCode = obisCode;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    /**
     * Get the inner message UUID (used for SECTION_UUID in curves table)
     * This is the UUID from the inner MessageHeader per channel/profile
     * 
     * @return The inner message UUID
     */
    public String getMessageUuid() {
        return messageUuid;
    }

    /**
     * Set the inner message UUID
     * 
     * @param messageUuid The inner message UUID
     */
    public void setMessageUuid(String messageUuid) {
        this.messageUuid = messageUuid;
    }

    public List<IntervalData> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<IntervalData> intervals) {
        this.intervals = intervals;
    }
    
    /**
     * Get the number of intervals
     */
    public int getIntervalCount() {
        return intervals != null ? intervals.size() : 0;
    }
    
    @Override
    public String toString() {
        return String.format("LoadProfileData[obis=%s, pod=%s, uuid=%s, intervals=%d]",
            obisCode, podId, messageUuid, getIntervalCount());
    }
}
