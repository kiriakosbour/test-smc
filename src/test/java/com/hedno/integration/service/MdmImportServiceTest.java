package com.hedno.integration.service;

import com.hedno.integration.processor.IntervalData;
import com.hedno.integration.processor.LoadProfileData;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

/**
 * Unit tests for MdmImportService
 */
public class MdmImportServiceTest {

    private MdmImportService service;

    @Before
    public void setUp() {
        service = new MdmImportService();
    }

    @Test
    public void testCurveRowCreation() {
        MdmImportService.CurveRow row = new MdmImportService.CurveRow();
        
        row.setHdLogId(1L);
        row.setPodId("GR123456789012345678901");
        row.setSupplyNum("456789012");
        row.setQValue(1, new BigDecimal("100.5"));
        row.setQValue(96, new BigDecimal("200.75"));
        row.setSValue(1, "W");
        row.setSValue(96, "V");

        assertEquals(1L, row.getHdLogId());
        assertEquals("GR123456789012345678901", row.getPodId());
        assertEquals("456789012", row.getSupplyNum());
        assertEquals(new BigDecimal("100.5"), row.getQValue(1));
        assertEquals(new BigDecimal("200.75"), row.getQValue(96));
        assertEquals("W", row.getSValue(1));
        assertEquals("V", row.getSValue(96));
        assertNull(row.getQValue(50)); // Not set
    }

    @Test
    public void testLoadProfileDataWithIntervals() {
        LoadProfileData profile = new LoadProfileData();
        profile.setMessageUuid("TEST-UUID-123");
        profile.setPodId("GR001234567890123456789");
        profile.setObisCode("1-0:1.8.0");
        profile.setSourceSystem("ZFA");

        // Add intervals
        for (int i = 0; i < 96; i++) {
            IntervalData interval = new IntervalData();
            interval.setStartDateTime(LocalDateTime.of(2024, 12, 5, i / 4, (i % 4) * 15));
            interval.setValue(new BigDecimal(i * 10));
            interval.setStatus("W");
            interval.setUnitCode("KWH");
            profile.addInterval(interval);
        }

        assertEquals(96, profile.getIntervals().size());
        assertEquals("TEST-UUID-123", profile.getMessageUuid());
        assertEquals("ZFA", profile.getSourceSystem());
    }

    @Test
    public void testIntervalDataCreation() {
        IntervalData interval = new IntervalData();
        interval.setStartDateTime(LocalDateTime.of(2024, 12, 5, 10, 30));
        interval.setValue(new BigDecimal("123.456"));
        interval.setStatus("V");
        interval.setUnitCode("KVARH");

        assertEquals(LocalDateTime.of(2024, 12, 5, 10, 30), interval.getStartDateTime());
        assertEquals(new BigDecimal("123.456"), interval.getValue());
        assertEquals("V", interval.getStatus());
        assertEquals("KVARH", interval.getUnitCode());
    }

    @Test
    public void testIntervalDataDefaultStatus() {
        IntervalData interval = new IntervalData();
        assertEquals("W", interval.getStatus()); // Default should be "W"
    }

    @Test
    public void testIntervalDataConstructor() {
        IntervalData interval = new IntervalData(
            LocalDateTime.of(2024, 12, 5, 14, 45),
            new BigDecimal("999.99"),
            "E"
        );

        assertEquals(LocalDateTime.of(2024, 12, 5, 14, 45), interval.getStartDateTime());
        assertEquals(new BigDecimal("999.99"), interval.getValue());
        assertEquals("E", interval.getStatus());
    }

    @Test
    public void testQIndexCalculation() {
        // Q1 should be for 00:00-00:14
        // Q2 should be for 00:15-00:29
        // Q96 should be for 23:45-23:59

        // Test Q index formula: (hour * 4) + (minute / 15) + 1
        assertEquals(1, calculateQIndex(0, 0));   // 00:00 -> Q1
        assertEquals(1, calculateQIndex(0, 14));  // 00:14 -> Q1
        assertEquals(2, calculateQIndex(0, 15));  // 00:15 -> Q2
        assertEquals(4, calculateQIndex(0, 45));  // 00:45 -> Q4
        assertEquals(5, calculateQIndex(1, 0));   // 01:00 -> Q5
        assertEquals(48, calculateQIndex(11, 45)); // 11:45 -> Q48
        assertEquals(49, calculateQIndex(12, 0));  // 12:00 -> Q49
        assertEquals(96, calculateQIndex(23, 45)); // 23:45 -> Q96
    }

    private int calculateQIndex(int hour, int minute) {
        return (hour * 4) + (minute / 15) + 1;
    }

    @Test
    public void testSupplyNumExtraction() {
        // POD format: 22 chars, SUPPLY_NUM is chars 4-12 (0-indexed: 3-11)
        String pod = "GR0123456789012345678901";
        String supplyNum = pod.substring(3, 12);
        assertEquals("123456789", supplyNum);
        assertEquals(9, supplyNum.length());
    }

    @Test
    public void testSourceSystemConstants() {
        assertEquals("ZFA", MdmImportService.SOURCE_SYSTEM_ZFA);
        assertEquals("ITRON", MdmImportService.SOURCE_SYSTEM_ITRON);
        assertEquals("MEASURE", MdmImportService.SOURCE_TYPE_MEASURE);
        assertEquals("ALARM", MdmImportService.SOURCE_TYPE_ALARM);
        assertEquals("EVENT", MdmImportService.SOURCE_TYPE_EVENT);
    }
}
