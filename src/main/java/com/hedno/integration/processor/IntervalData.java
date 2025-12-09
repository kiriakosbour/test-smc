package com.hedno.integration.processor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Interval Data entity.
 * Represents a single 15-minute interval reading.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class IntervalData {

    private LocalDateTime startDateTime;
    private BigDecimal value;
    private String unitCode;
    private String status;

    public IntervalData() {
        this.status = "W"; // Default status
    }

    public IntervalData(LocalDateTime startDateTime, BigDecimal value, String status) {
        this.startDateTime = startDateTime;
        this.value = value;
        this.status = status != null ? status : "W";
    }

    // Getters and Setters

    public LocalDateTime getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(LocalDateTime startDateTime) {
        this.startDateTime = startDateTime;
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

    @Override
    public String toString() {
        return "IntervalData{" +
                "startDateTime=" + startDateTime +
                ", value=" + value +
                ", unitCode='" + unitCode + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
