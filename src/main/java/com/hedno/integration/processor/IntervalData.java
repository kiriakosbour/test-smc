package com.hedno.integration.processor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Data Transfer Object for a single profile interval.
 * Extracted from OrderProcessor to be a top-level class.
 */
public class IntervalData {

    private LocalDateTime startDateTime;
    private LocalDateTime endDateTime;
    private BigDecimal value;
    private String unitCode;
    private String status;

    // Getters and Setters

    public LocalDateTime getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(LocalDateTime startDateTime) {
        this.startDateTime = startDateTime;
    }

    public LocalDateTime getEndDateTime() {
        return endDateTime;
    }

    public void setEndDateTime(LocalDateTime endDateTime) {
        this.endDateTime = endDateTime;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public String getUnitCode() {
        return unitCode;
    }

    public void setUnitCode(String unitCode) {
        this.unitCode = unitCode;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}