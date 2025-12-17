package com.hedno.integration.dao;

import java.math.BigDecimal;

public class ReadingDAO {
    public final Double value;
    //public final BigDecimal value2;
    //public final BigDecimal value3;
    public final String statusRef;
    public final String readingTime; // may be null

    public ReadingDAO(Double value, String statusRef, String readingTime) {
        this.value = value;
        //this.value2 = value2;
        //this.value3 = value3;
        this.statusRef = statusRef;
        this.readingTime = readingTime;
    }

    @Override
    public String toString() {
        return "Reading{value=" + value + ", statusRef=" + statusRef + ", readingTime=" + readingTime + "}";
    }
}
