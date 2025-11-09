package com.hedno.integration.service;

import com.hedno.integration.service.XMLBuilderService.LoadProfileData;
import com.hedno.integration.service.XMLBuilderService.IntervalData;
import com.hedno.integration.soap.model.UtilitiesTimeSeriesERPItemBulkNotification;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for XMLBuilderService
 * 
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class XMLBuilderServiceTest {
    
    private static final Logger logger = LoggerFactory.getLogger(XMLBuilderServiceTest.class);
    
    private XMLBuilderService xmlBuilder;
    
    @Before
    public void setup() throws JAXBException, DatatypeConfigurationException {
        xmlBuilder = new XMLBuilderService(10); // Max 10 profiles per message
    }
    
    /**
     * Test building a valid bulk notification with single profile
     */
    @Test
    public void testBuildBulkNotificationSingleProfile() {
        // Prepare test data
        LoadProfileData profileData = createTestProfileData(
            "1.29.99.128", 
            "HU000130F110S-TEST-001",
            96 // Normal day with 96 intervals
        );
        
        List<LoadProfileData> profileList = new ArrayList<>();
        profileList.add(profileData);
        
        // Build notification
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        
        // Verify structure
        assertNotNull("Notification should not be null", notification);
        assertNotNull("Message header should not be null", notification.getMessageHeader());
        assertNotNull("UUID should not be null", notification.getMessageHeader().getUuid());
        assertEquals("UUID should be 36 characters", 36, 
            notification.getMessageHeader().getUuid().length());
        
        // Verify sender and recipient
        assertEquals("Sender should be T_MERES", "T_MERES", 
            notification.getMessageHeader().getSenderParty().getStandardID().getValue());
        assertEquals("Recipient should be EHE000130", "EHE000130", 
            notification.getMessageHeader().getRecipientParty().getStandardID().getValue());
        
        // Verify notification messages
        assertNotNull("Notification messages should not be null", 
            notification.getNotificationMessages());
        assertEquals("Should have 1 notification message", 1, 
            notification.getNotificationMessages().size());
        
        // Verify time series data
        UtilitiesTimeSeriesERPItemBulkNotification.UtilitiesTimeSeriesERPItemNotificationMessage msg = 
            notification.getNotificationMessages().get(0);
        assertNotNull("Time series should not be null", msg.getUtilitiesTimeSeries());
        assertEquals("Should have 96 intervals", 96, 
            msg.getUtilitiesTimeSeries().getItems().size());
        
        // Verify meter identification
        assertEquals("OBIS code should match", "1.29.99.128", 
            msg.getUtilitiesTimeSeries().getMeasurementRole().getObisCode());
        assertEquals("POD ID should match", "HU000130F110S-TEST-001", 
            msg.getUtilitiesTimeSeries().getMeasurementRole()
                .getPointOfDeliveryIdentification().getPodId());
    }
    
    /**
     * Test building with multiple profiles
     */
    @Test
    public void testBuildBulkNotificationMultipleProfiles() {
        List<LoadProfileData> profileList = new ArrayList<>();
        
        // Add 5 profiles
        for (int i = 1; i <= 5; i++) {
            LoadProfileData profileData = createTestProfileData(
                "1.29.99." + (127 + i),
                "HU000130F110S-TEST-00" + i,
                96
            );
            profileList.add(profileData);
        }
        
        // Build notification
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        
        // Verify
        assertNotNull("Notification should not be null", notification);
        assertEquals("Should have 5 notification messages", 5, 
            notification.getNotificationMessages().size());
        
        // All messages should share the same main UUID
        String mainUuid = notification.getMessageHeader().getUuid();
        for (UtilitiesTimeSeriesERPItemBulkNotification.UtilitiesTimeSeriesERPItemNotificationMessage msg : notification.getNotificationMessages()) {
            assertEquals("All messages should have same UUID", mainUuid, 
                msg.getMessageHeader().getUuid());
        }
    }
    
    /**
     * Test exceeding maximum profiles per message
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExceedMaxProfilesPerMessage() {
        List<LoadProfileData> profileList = new ArrayList<>();
        
        // Add 11 profiles (exceeds max of 10)
        for (int i = 1; i <= 11; i++) {
            LoadProfileData profileData = createTestProfileData(
                "1.29.99." + (127 + i),
                "HU000130F110S-TEST-0" + i,
                96
            );
            profileList.add(profileData);
        }
        
        // Should throw exception
        xmlBuilder.buildBulkNotification(profileList);
    }
    
    /**
     * Test XML marshalling
     */
    @Test
    public void testMarshalToXml() throws JAXBException {
        // Build a simple notification
        LoadProfileData profileData = createTestProfileData(
            "1.29.99.128", 
            "HU000130F110S-TEST-001",
            4 // Just 4 intervals for simplicity
        );
        
        List<LoadProfileData> profileList = new ArrayList<>();
        profileList.add(profileData);
        
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        
        // Marshal to XML
        String xml = xmlBuilder.marshalToXml(notification);
        
        // Verify XML structure
        assertNotNull("XML should not be null", xml);
        assertTrue("XML should contain SOAP envelope", 
            xml.contains("<soapenv:Envelope"));
        assertTrue("XML should contain namespace", 
            xml.contains("xmlns:glob=\"http://sap.com/xi/SAPGlobal20/Global\""));
        assertTrue("XML should contain UUID", 
            xml.contains(notification.getMessageHeader().getUuid()));
        assertTrue("XML should contain sender", 
            xml.contains("T_MERES"));
        assertTrue("XML should contain recipient", 
            xml.contains("EHE000130"));
        
        logger.info("Generated XML length: {} characters", xml.length());
    }
    
    /**
     * Test XML validation
     */
    @Test
    public void testValidateXml() throws JAXBException {
        // Build valid notification
        LoadProfileData profileData = createTestProfileData(
            "1.29.99.128", 
            "HU000130F110S-TEST-001",
            96
        );
        
        List<LoadProfileData> profileList = new ArrayList<>();
        profileList.add(profileData);
        
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        
        // Marshal and validate
        String xml = xmlBuilder.marshalToXml(notification);
        boolean isValid = xmlBuilder.validateXml(xml);
        
        assertTrue("Valid XML should pass validation", isValid);
    }
    
    /**
     * Test extracting UUID from XML
     */
    @Test
    public void testExtractMessageUuid() throws JAXBException {
        // Build notification with known UUID
        LoadProfileData profileData = createTestProfileData(
            "1.29.99.128", 
            "HU000130F110S-TEST-001",
            96
        );
        
        List<LoadProfileData> profileList = new ArrayList<>();
        profileList.add(profileData);
        
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        String expectedUuid = notification.getMessageHeader().getUuid();
        
        // Marshal to XML
        String xml = xmlBuilder.marshalToXml(notification);
        
        // Extract UUID
        String extractedUuid = xmlBuilder.extractMessageUuid(xml);
        
        assertNotNull("Extracted UUID should not be null", extractedUuid);
        assertEquals("Extracted UUID should match", expectedUuid, extractedUuid);
    }
    
    /**
     * Test handling DST change with 100 intervals
     */
    @Test
    public void testDSTChangeWith100Intervals() {
        // Create profile with 100 intervals (DST spring forward)
        LoadProfileData profileData = createTestProfileData(
            "1.29.99.128", 
            "HU000130F110S-DST-TEST",
            100 // DST change day
        );
        
        List<LoadProfileData> profileList = new ArrayList<>();
        profileList.add(profileData);
        
        // Build notification
        UtilitiesTimeSeriesERPItemBulkNotification notification = 
            xmlBuilder.buildBulkNotification(profileList);
        
        // Verify
        UtilitiesTimeSeriesERPItemBulkNotification.UtilitiesTimeSeriesERPItemNotificationMessage msg = 
            notification.getNotificationMessages().get(0);
        assertEquals("Should have 100 intervals for DST change", 100, 
            msg.getUtilitiesTimeSeries().getItems().size());
    }
    
    /**
     * Test empty profile list
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEmptyProfileList() {
        List<LoadProfileData> emptyList = new ArrayList<>();
        xmlBuilder.buildBulkNotification(emptyList);
    }
    
    /**
     * Test null profile list
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullProfileList() {
        xmlBuilder.buildBulkNotification(null);
    }
    
    // ===== Helper Methods =====
    
    /**
     * Create test profile data with specified parameters
     */
    private LoadProfileData createTestProfileData(String obisCode, String podId, int intervalCount) {
        LoadProfileData profileData = new LoadProfileData();
        profileData.setObisCode(obisCode);
        profileData.setPodId(podId);
        
        List<IntervalData> intervals = new ArrayList<>();
        LocalDateTime startTime = LocalDateTime.of(2025, Month.JANUARY, 4, 0, 0, 0);
        
        for (int i = 0; i < intervalCount; i++) {
            IntervalData interval = new IntervalData();
            
            // Calculate times (15-minute intervals)
            LocalDateTime intervalStart = startTime.plusMinutes(i * 15);
            LocalDateTime intervalEnd = intervalStart.plusMinutes(15).minusSeconds(1);
            
            interval.setStartDateTime(intervalStart);
            interval.setEndDateTime(intervalEnd);
            interval.setValue(new BigDecimal("5.123456")); // Sample value
            interval.setUnitCode("KWH");
            interval.setStatus("W");
            
            intervals.add(interval);
        }
        
        profileData.setIntervals(intervals);
        return profileData;
    }
}
