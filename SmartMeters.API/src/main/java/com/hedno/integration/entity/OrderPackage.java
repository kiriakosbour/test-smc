package com.hedno.integration.entity;

import java.sql.Timestamp;

/**
 * Entity class representing the SMC_ORDER_PACKAGES table.
 * This class holds a batch of "order items" to be processed.
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class OrderPackage {

    private long packageId;
    private PackageStatus status;
    private Timestamp createdTimestamp;
    private String channelId;

    /**
     * Enum for package processing states, matching the database constraint.
     */
    public enum PackageStatus {
        OPEN("OPEN"),
        PROCESSING("PROCESSING"),
        FAILED("FAILED"),
        COMPLETED("COMPLETED");

        private final String value;

        PackageStatus(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static PackageStatus fromValue(String value) {
            for (PackageStatus status : PackageStatus.values()) {
                if (status.value.equals(value)) {
                    return status;
                }
            }
            throw new IllegalArgumentException("Invalid package status: " + value);
        }
    }

    // Getters and Setters

    public long getPackageId() {
        return packageId;
    }

    public void setPackageId(long packageId) {
        this.packageId = packageId;
    }

    public PackageStatus getStatus() {
        return status;
    }

    public void setStatus(PackageStatus status) {
        this.status = status;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Timestamp createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    @Override
    public String toString() {
        return "OrderPackage{" +
                "packageId=" + packageId +
                ", status=" + status +
                ", createdTimestamp=" + createdTimestamp +
                ", channelId='" + channelId + '\'' +
                '}';
    }
}