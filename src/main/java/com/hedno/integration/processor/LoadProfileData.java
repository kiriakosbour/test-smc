package com.hedno.integration.processor;

import java.util.ArrayList;
import java.util.List;

/**
 * Load Profile Data entity.
 * Represents a single meter channel's data from MDM.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class LoadProfileData {

    private String messageUuid;
    private String podId;
    private String obisCode;
    private String sourceSystem;
    private List<IntervalData> intervals;

    public LoadProfileData() {
        this.intervals = new ArrayList<>();
    }

    // Getters and Setters

    public String getMessageUuid() {
        return messageUuid;
    }

    public void setMessageUuid(String messageUuid) {
        this.messageUuid = messageUuid;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public String getObisCode() {
        return obisCode;
    }

    public void setObisCode(String obisCode) {
        this.obisCode = obisCode;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public List<IntervalData> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<IntervalData> intervals) {
        this.intervals = intervals;
    }

    public void addInterval(IntervalData interval) {
        if (this.intervals == null) {
            this.intervals = new ArrayList<>();
        }
        this.intervals.add(interval);
    }

    @Override
    public String toString() {
        return "LoadProfileData{" +
                "messageUuid='" + messageUuid + '\'' +
                ", podId='" + podId + '\'' +
                ", obisCode='" + obisCode + '\'' +
                ", sourceSystem='" + sourceSystem + '\'' +
                ", intervalsCount=" + (intervals != null ? intervals.size() : 0) +
                '}';
    }
}
