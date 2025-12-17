package com.hedno.integration.dao;

import java.util.List;
import java.util.ArrayList;

public class ChannelDAO {
    public final String servicePointChannelId;
    public final String startDate;
    public final String endDate;
    public final String timeZone;
    public final Integer intervalLength; // nullable
    public final String isRegister;

    public final List<ReadingDAO> lstReadings = new ArrayList<>();

    public ChannelDAO(String servicePointChannelId, String startDate, String endDate,
                      String timeZone, Integer intervalLength, String isRegister) {
        this.servicePointChannelId = servicePointChannelId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.timeZone = timeZone;
        this.intervalLength = intervalLength;
        this.isRegister = isRegister;
    }
}
