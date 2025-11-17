package com.hedno.integration.soap.model;

import javax.xml.bind.annotation.*;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Root element for UtilitiesTimeSeriesERPItemBulkNotification SOAP message.
 * This class structure maps to the WSDL requirements.
 * 
 * @author HEDNO Integration Team
 * @version 1.0
 */
@XmlRootElement(name = "UtilitiesTimeSeriesERPItemBulkNotification", 
                namespace = "http://sap.com/xi/SAPGlobal20/Global")
@XmlAccessorType(XmlAccessType.FIELD)
public class UtilitiesTimeSeriesERPItemBulkNotification {
    
    @XmlElement(name  = "MessageHeader", namespace = "http://sap.com/xi/SAPGlobal20/Global", required = true)
    private MessageHeader messageHeader;
    
    @XmlElement(name  = "UtilitiesTimeSeriesERPItemNotificationMessage", 
                namespace = "http://sap.com/xi/SAPGlobal20/Global")
    private List<UtilitiesTimeSeriesERPItemNotificationMessage> notificationMessages;
    
    public UtilitiesTimeSeriesERPItemBulkNotification() {
        this.notificationMessages = new ArrayList<>();
    }
    
    // Getters and Setters
    public MessageHeader getMessageHeader() {
        return messageHeader;
    }
    
    public void setMessageHeader(MessageHeader messageHeader) {
        this.messageHeader = messageHeader;
    }
    
    public List<UtilitiesTimeSeriesERPItemNotificationMessage> getNotificationMessages() {
        if (notificationMessages == null) {
            notificationMessages = new ArrayList<>();
        }
        return notificationMessages;
    }
    
    public void setNotificationMessages(List<UtilitiesTimeSeriesERPItemNotificationMessage> notificationMessages) {
        this.notificationMessages = notificationMessages;
    }
    
    /**
     * Main Message Header
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "MessageHeader", namespace = "http://sap.com/xi/SAPGlobal20/Global")
    public static class MessageHeader {
        
        @XmlElement(name  = "UUID", required = true)
        private String uuid;
        
        @XmlElement(name  = "ReferenceUUID")
        private String referenceUuid;
        
        @XmlElement(name  = "CreationDateTime", required = true)
        private XMLGregorianCalendar creationDateTime;
        
        @XmlElement(name  = "SenderParty", required = true)
        private Party senderParty;
        
        @XmlElement(name  = "RecipientParty", required = true)
        private Party recipientParty;
        
        // Getters and Setters
        public String getUuid() {
            return uuid;
        }
        
        public void setUuid(String uuid) {
            this.uuid = uuid;
        }
        
        public String getReferenceUuid() {
            return referenceUuid;
        }
        
        public void setReferenceUuid(String referenceUuid) {
            this.referenceUuid = referenceUuid;
        }
        
        public XMLGregorianCalendar getCreationDateTime() {
            return creationDateTime;
        }
        
        public void setCreationDateTime(XMLGregorianCalendar creationDateTime) {
            this.creationDateTime = creationDateTime;
        }
        
        public Party getSenderParty() {
            return senderParty;
        }
        
        public void setSenderParty(Party senderParty) {
            this.senderParty = senderParty;
        }
        
        public Party getRecipientParty() {
            return recipientParty;
        }
        
        public void setRecipientParty(Party recipientParty) {
            this.recipientParty = recipientParty;
        }
    }
    
    /**
     * Party information (Sender/Recipient)
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "Party")
    public static class Party {
        
        @XmlElement(name  = "StandardID", required = true)
        private StandardID standardID;
        
        public StandardID getStandardID() {
            return standardID;
        }
        
        public void setStandardID(StandardID standardID) {
            this.standardID = standardID;
        }
        
        @XmlAccessorType(XmlAccessType.FIELD)
        public static class StandardID {
            
            @XmlValue
            private String value;
            
            @XmlAttribute(name  = "schemeAgencyID")
            private String schemeAgencyID = "";
            
            public StandardID() {}
            
            public StandardID(String value) {
                this.value = value;
                this.schemeAgencyID = "";
            }
            
            public String getValue() {
                return value;
            }
            
            public void setValue(String value) {
                this.value = value;
            }
            
            public String getSchemeAgencyID() {
                return schemeAgencyID;
            }
            
            public void setSchemeAgencyID(String schemeAgencyID) {
                this.schemeAgencyID = schemeAgencyID;
            }
        }
    }
    
    /**
     * Individual notification message for each profile
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "UtilitiesTimeSeriesERPItemNotificationMessage")
    public static class UtilitiesTimeSeriesERPItemNotificationMessage {
        
        @XmlElement(name  = "MessageHeader", namespace = "http://sap.com/xi/SAPGlobal20/Global", required = true)
        private MessageHeader messageHeader;
        
        @XmlElement(name  = "UtilitiesTimeSeries", namespace = "http://sap.com/xi/SAPGlobal20/Global", required = true)
        private UtilitiesTimeSeries utilitiesTimeSeries;
        
        // Getters and Setters
        public MessageHeader getMessageHeader() {
            return messageHeader;
        }
        
        public void setMessageHeader(MessageHeader messageHeader) {
            this.messageHeader = messageHeader;
        }
        
        public UtilitiesTimeSeries getUtilitiesTimeSeries() {
            return utilitiesTimeSeries;
        }
        
        public void setUtilitiesTimeSeries(UtilitiesTimeSeries utilitiesTimeSeries) {
            this.utilitiesTimeSeries = utilitiesTimeSeries;
        }
    }
    
    /**
     * Time series data container
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "UtilitiesTimeSeries")
    public static class UtilitiesTimeSeries {
        
        @XmlElement(name  = "Item")
        private List<TimeSeriesItem> items;
        
        @XmlElement(name  = "UtilitiesMeasurementTaskAssignmentRole", required = true)
        private UtilitiesMeasurementTaskAssignmentRole measurementRole;
        
        public UtilitiesTimeSeries() {
            this.items = new ArrayList<>();
        }
        
        public List<TimeSeriesItem> getItems() {
            if (items == null) {
                items = new ArrayList<>();
            }
            return items;
        }
        
        public void setItems(List<TimeSeriesItem> items) {
            this.items = items;
        }
        
        public UtilitiesMeasurementTaskAssignmentRole getMeasurementRole() {
            return measurementRole;
        }
        
        public void setMeasurementRole(UtilitiesMeasurementTaskAssignmentRole measurementRole) {
            this.measurementRole = measurementRole;
        }
    }
    
    /**
     * Individual time series item (one per interval)
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "Item")
    public static class TimeSeriesItem {
        
        @XmlElement(name  = "Quantity", required = true)
        private Quantity quantity;
        
        @XmlElement(name  = "UTCValidityStartDateTime", required = true)
        private XMLGregorianCalendar utcValidityStartDateTime;
        
        @XmlElement(name  = "UTCValidityEndDateTime", required = true)
        private XMLGregorianCalendar utcValidityEndDateTime;
        
        @XmlElement(name  = "ItemStatus")
        private ItemStatus itemStatus;
        
        // Getters and Setters
        public Quantity getQuantity() {
            return quantity;
        }
        
        public void setQuantity(Quantity quantity) {
            this.quantity = quantity;
        }
        
        public XMLGregorianCalendar getUtcValidityStartDateTime() {
            return utcValidityStartDateTime;
        }
        
        public void setUtcValidityStartDateTime(XMLGregorianCalendar utcValidityStartDateTime) {
            this.utcValidityStartDateTime = utcValidityStartDateTime;
        }
        
        public XMLGregorianCalendar getUtcValidityEndDateTime() {
            return utcValidityEndDateTime;
        }
        
        public void setUtcValidityEndDateTime(XMLGregorianCalendar utcValidityEndDateTime) {
            this.utcValidityEndDateTime = utcValidityEndDateTime;
        }
        
        public ItemStatus getItemStatus() {
            return itemStatus;
        }
        
        public void setItemStatus(ItemStatus itemStatus) {
            this.itemStatus = itemStatus;
        }
    }
    
    /**
     * Quantity with unit code
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "Quantity")
    public static class Quantity {
        
        @XmlValue
        private BigDecimal value;
        
        @XmlAttribute(name  = "unitCode")
        private String unitCode;
        
        public Quantity() {}
        
        public Quantity(BigDecimal value, String unitCode) {
            this.value = value;
            this.unitCode = unitCode;
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
    }
    
    /**
     * Item status
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "ItemStatus")
    public static class ItemStatus {
        
        @XmlElement(name  = "UtilitiesTimeSeriesItemTypeCode")
        private String utilitiesTimeSeriesItemTypeCode = "W"; // Default status
        
        public String getUtilitiesTimeSeriesItemTypeCode() {
            return utilitiesTimeSeriesItemTypeCode;
        }
        
        public void setUtilitiesTimeSeriesItemTypeCode(String utilitiesTimeSeriesItemTypeCode) {
            this.utilitiesTimeSeriesItemTypeCode = utilitiesTimeSeriesItemTypeCode;
        }
    }
    
    /**
     * Measurement task assignment role (meter identification)
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name  = "UtilitiesMeasurementTaskAssignmentRole")
    public static class UtilitiesMeasurementTaskAssignmentRole {
        
        @XmlElement(name  = "UtilitiesObjectIdentificationSystemCodeText", required = true)
        private String obisCode;
        
        @XmlElement(name  = "UtilitiesPointOfDeliveryExternalIdentification", required = true)
        private PointOfDeliveryIdentification pointOfDeliveryIdentification;
        
        public String getObisCode() {
            return obisCode;
        }
        
        public void setObisCode(String obisCode) {
            this.obisCode = obisCode;
        }
        
        public PointOfDeliveryIdentification getPointOfDeliveryIdentification() {
            return pointOfDeliveryIdentification;
        }
        
        public void setPointOfDeliveryIdentification(PointOfDeliveryIdentification pointOfDeliveryIdentification) {
            this.pointOfDeliveryIdentification  = pointOfDeliveryIdentification;
        }
        
        @XmlAccessorType(XmlAccessType.FIELD)
        public static class PointOfDeliveryIdentification {
            
            @XmlElement(name  = "UtilitiesPointOfDeliveryPartyID", required = true)
            private String podId;
            
            public String getPodId() {
                return podId;
            }
            
            public void setPodId(String podId) {
                this.podId = podId;
            }
        }
    }
}
