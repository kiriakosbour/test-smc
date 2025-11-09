package com.hedno.integration.entity;

import java.sql.Timestamp;

/**
 * Entity class representing the SMC_ORDER_ITEMS table.
 * This class holds a single piece of data (e.g., from PROFIL_BLOC)
 * that is part of an OrderPackage.
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class OrderItem {

    private long itemId;
    private Long packageId; // Can be null if not yet assigned
    private String profilBlocId;
    private String dataType;
    private String obisCode;
    private String podId;
    private ItemStatus status;
    private Timestamp createdTimestamp;

    /**
     * Enum for item processing states, matching the database constraint.
     */
    public enum ItemStatus {
        PENDING("PENDING"),
        PROCESSED("PROCESSED");

        private final String value;

        ItemStatus(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static ItemStatus fromValue(String value) {
            for (ItemStatus status : ItemStatus.values()) {
                if (status.value.equals(value)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid item status: " + value);
        }
    }

    // Getters and Setters

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public Long getPackageId() {
        return packageId;
    }

    public void setPackageId(Long packageId) {
        this.packageId = packageId;
    }

    public String getProfilBlocId() {
        return profilBlocId;
    }

    public void setProfilBlocId(String profilBlocId) {
        this.profilBlocId = profilBlocId;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

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

    public ItemStatus getStatus() {
        return status;
    }

    public void setStatus(ItemStatus status) {
        this.status = status;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Timestamp createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @Override
    public String toString() {
        return "OrderItem{" +
                "itemId=" + itemId +
                ", packageId=" + packageId +
                ", profilBlocId='" + profilBlocId + '\'' +
                ", dataType='" + dataType + '\'' +
                ", status=" + status +
                '}';
    }
}