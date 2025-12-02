package com.hedno.integration.processor;

import java.util.List;

/**
 * Data Transfer Object for a single load profile.
 * Extracted from OrderProcessor to be a top-level class.
 */
public class LoadProfileData {

    private String obisCode;
    private String podId;
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

    public List<IntervalData> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<IntervalData> intervals) {
        this.intervals = intervals;
    }
}