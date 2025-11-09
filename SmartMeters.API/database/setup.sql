-- =============================================================================
-- Load Profile Push API - Database Setup Script
-- Oracle Database 12c or higher
-- 
-- This script creates the necessary database objects for the Load Profile
-- Push API system. Run this script with appropriate DBA privileges.
-- =============================================================================

-- Create user/schema (if not exists)
-- Note: Adjust password and tablespace names according to your environment
/*
CREATE USER LOAD_PROFILE IDENTIFIED BY "SecurePassword123!"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO LOAD_PROFILE;
GRANT CREATE SESSION TO LOAD_PROFILE;
GRANT CREATE TABLE TO LOAD_PROFILE;
GRANT CREATE SEQUENCE TO LOAD_PROFILE;
GRANT CREATE PROCEDURE TO LOAD_PROFILE;
GRANT CREATE TRIGGER TO LOAD_PROFILE;
GRANT CREATE VIEW TO LOAD_PROFILE;
*/

-- Connect as LOAD_PROFILE user
-- ALTER SESSION SET CURRENT_SCHEMA = LOAD_PROFILE;

-- =============================================================================
-- Drop existing objects (BE CAREFUL IN PRODUCTION!)
-- =============================================================================
-- DROP TABLE LOAD_PROFILE_INBOUND CASCADE CONSTRAINTS;
-- DROP SEQUENCE SEQ_LOAD_PROFILE_AUDIT;
-- DROP TABLE LOAD_PROFILE_AUDIT;

-- =============================================================================
-- Main Table: LOAD_PROFILE_INBOUND
-- Stores incoming load profile messages for processing
-- =============================================================================
CREATE TABLE LOAD_PROFILE_INBOUND (
    -- Primary Key, from the main <MessageHeader><UUID> in the XML
    MESSAGE_UUID VARCHAR2(36) NOT NULL,
    
    -- The complete raw XML payload, stored for auditing and reprocessing
    RAW_PAYLOAD CLOB NOT NULL,
    
    -- Tracks the message lifecycle, used by the portal and processors
    STATUS VARCHAR2(20) NOT NULL,
    
    -- Audit Timestamps
    RECEIVED_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    LAST_ATTEMPT_TIMESTAMP TIMESTAMP,
    
    -- Retry & Error Info
    ATTEMPT_COUNT NUMBER(3) DEFAULT 0 NOT NULL,
    LAST_ERROR_MESSAGE VARCHAR2(4000),
    
    -- Constraints
    CONSTRAINT PK_LOAD_PROFILE_INBOUND PRIMARY KEY (MESSAGE_UUID),
    CONSTRAINT CHK_STATUS CHECK (STATUS IN ('RECEIVED', 'PROCESSING', 'SENT_OK', 'FAILED_RETRY', 'FAILED_DLQ')),
    CONSTRAINT CHK_ATTEMPT_COUNT CHECK (ATTEMPT_COUNT >= 0)
) TABLESPACE USERS;

-- Add comments
COMMENT ON TABLE LOAD_PROFILE_INBOUND IS 'Stores incoming load profile SOAP messages for processing and delivery to SAP';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.MESSAGE_UUID IS 'Unique UUID from the inbound glob:MessageHeader';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.RAW_PAYLOAD IS 'The raw SOAP XML payload for auditing and manual resend';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.STATUS IS 'Current processing state of the message';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.RECEIVED_TIMESTAMP IS 'Timestamp when message was first received';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.LAST_ATTEMPT_TIMESTAMP IS 'Timestamp of last processing attempt';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.ATTEMPT_COUNT IS 'Number of processing attempts made';
COMMENT ON COLUMN LOAD_PROFILE_INBOUND.LAST_ERROR_MESSAGE IS 'Error message from last failed attempt';

-- =============================================================================
-- Indexes for Performance
-- =============================================================================

-- Index for finding messages by status (used by processor)
CREATE INDEX IDX_LOAD_PROFILE_STATUS 
ON LOAD_PROFILE_INBOUND (STATUS, RECEIVED_TIMESTAMP)
TABLESPACE USERS;

-- Index for retry processing
CREATE INDEX IDX_LOAD_PROFILE_RETRY
ON LOAD_PROFILE_INBOUND (STATUS, ATTEMPT_COUNT, LAST_ATTEMPT_TIMESTAMP)
WHERE STATUS IN ('RECEIVED', 'FAILED_RETRY')
TABLESPACE USERS;

-- Index for monitoring/reporting
CREATE INDEX IDX_LOAD_PROFILE_RECEIVED
ON LOAD_PROFILE_INBOUND (RECEIVED_TIMESTAMP DESC)
TABLESPACE USERS;

-- =============================================================================
-- Audit Table (Optional but recommended)
-- =============================================================================
CREATE SEQUENCE SEQ_LOAD_PROFILE_AUDIT START WITH 1 INCREMENT BY 1;

CREATE TABLE LOAD_PROFILE_AUDIT (
    AUDIT_ID NUMBER DEFAULT SEQ_LOAD_PROFILE_AUDIT.NEXTVAL NOT NULL,
    MESSAGE_UUID VARCHAR2(36) NOT NULL,
    ACTION VARCHAR2(50) NOT NULL,
    OLD_STATUS VARCHAR2(20),
    NEW_STATUS VARCHAR2(20),
    ACTION_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    ACTION_BY VARCHAR2(100),
    DETAILS VARCHAR2(4000),
    
    CONSTRAINT PK_LOAD_PROFILE_AUDIT PRIMARY KEY (AUDIT_ID)
) TABLESPACE USERS;

CREATE INDEX IDX_AUDIT_MESSAGE_UUID ON LOAD_PROFILE_AUDIT (MESSAGE_UUID);
CREATE INDEX IDX_AUDIT_TIMESTAMP ON LOAD_PROFILE_AUDIT (ACTION_TIMESTAMP DESC);

COMMENT ON TABLE LOAD_PROFILE_AUDIT IS 'Audit trail for load profile message processing';

-- =============================================================================
-- Configuration Table (Optional)
-- =============================================================================
CREATE TABLE LOAD_PROFILE_CONFIG (
    CONFIG_KEY VARCHAR2(100) NOT NULL,
    CONFIG_VALUE VARCHAR2(500),
    CONFIG_DESC VARCHAR2(1000),
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP,
    UPDATED_BY VARCHAR2(100),
    
    CONSTRAINT PK_LOAD_PROFILE_CONFIG PRIMARY KEY (CONFIG_KEY)
) TABLESPACE USERS;

-- Insert default configuration
INSERT INTO LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES
    ('PROCESSOR_INTERVAL_MS', '60000', 'Processing interval in milliseconds'),
    ('BATCH_SIZE', '10', 'Number of messages to process in each batch'),
    ('MAX_RETRIES', '3', 'Maximum retry attempts before moving to DLQ'),
    ('MAX_PROFILES_PER_MESSAGE', '10', 'Maximum profiles per SOAP message'),
    ('SAP_ENDPOINT_HOST', 'sapd2ojas67.sapservers.local', 'SAP endpoint hostname'),
    ('SAP_ENDPOINT_PORT', '56700', 'SAP endpoint port'),
    ('SAP_ENDPOINT_PATH', '/XISOAPAdapter/MessageServlet', 'SAP endpoint path'),
    ('USE_HTTPS', 'true', 'Use HTTPS for SAP communication'),
    ('CONNECT_TIMEOUT_MS', '30000', 'Connection timeout in milliseconds'),
    ('READ_TIMEOUT_MS', '60000', 'Read timeout in milliseconds');

COMMIT;

-- =============================================================================
-- Statistics View
-- =============================================================================
CREATE OR REPLACE VIEW V_LOAD_PROFILE_STATS AS
SELECT 
    STATUS,
    COUNT(*) AS MESSAGE_COUNT,
    MIN(RECEIVED_TIMESTAMP) AS OLDEST_MESSAGE,
    MAX(RECEIVED_TIMESTAMP) AS NEWEST_MESSAGE,
    AVG(ATTEMPT_COUNT) AS AVG_ATTEMPTS
FROM LOAD_PROFILE_INBOUND
GROUP BY STATUS;

COMMENT ON VIEW V_LOAD_PROFILE_STATS IS 'Statistics view for load profile message processing';

-- =============================================================================
-- Monitoring Views
-- =============================================================================

-- Messages requiring attention
CREATE OR REPLACE VIEW V_LOAD_PROFILE_ATTENTION AS
SELECT 
    MESSAGE_UUID,
    STATUS,
    RECEIVED_TIMESTAMP,
    LAST_ATTEMPT_TIMESTAMP,
    ATTEMPT_COUNT,
    SUBSTR(LAST_ERROR_MESSAGE, 1, 200) AS ERROR_SUMMARY,
    CASE 
        WHEN STATUS = 'FAILED_DLQ' THEN 'DEAD LETTER'
        WHEN STATUS = 'FAILED_RETRY' AND ATTEMPT_COUNT >= 3 THEN 'MAX RETRIES'
        WHEN STATUS = 'PROCESSING' AND LAST_ATTEMPT_TIMESTAMP < SYSTIMESTAMP - INTERVAL '30' MINUTE THEN 'STUCK'
        ELSE 'PENDING'
    END AS ATTENTION_TYPE
FROM LOAD_PROFILE_INBOUND
WHERE STATUS IN ('FAILED_RETRY', 'FAILED_DLQ', 'PROCESSING')
ORDER BY RECEIVED_TIMESTAMP;

-- Daily processing summary
CREATE OR REPLACE VIEW V_LOAD_PROFILE_DAILY_SUMMARY AS
SELECT 
    TRUNC(RECEIVED_TIMESTAMP) AS PROCESS_DATE,
    COUNT(*) AS TOTAL_MESSAGES,
    SUM(CASE WHEN STATUS = 'SENT_OK' THEN 1 ELSE 0 END) AS SUCCESSFUL,
    SUM(CASE WHEN STATUS IN ('FAILED_RETRY', 'FAILED_DLQ') THEN 1 ELSE 0 END) AS FAILED,
    SUM(CASE WHEN STATUS IN ('RECEIVED', 'PROCESSING') THEN 1 ELSE 0 END) AS PENDING,
    ROUND(AVG(ATTEMPT_COUNT), 2) AS AVG_ATTEMPTS
FROM LOAD_PROFILE_INBOUND
GROUP BY TRUNC(RECEIVED_TIMESTAMP)
ORDER BY PROCESS_DATE DESC;

-- =============================================================================
-- Stored Procedures (Optional)
-- =============================================================================

-- Procedure to cleanup old successful messages
CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_MESSAGES(
    p_days_to_keep IN NUMBER DEFAULT 30,
    p_deleted_count OUT NUMBER
) AS
BEGIN
    DELETE FROM LOAD_PROFILE_INBOUND
    WHERE STATUS = 'SENT_OK'
    AND RECEIVED_TIMESTAMP < SYSTIMESTAMP - INTERVAL '1' DAY * p_days_to_keep;
    
    p_deleted_count := SQL%ROWCOUNT;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Deleted ' || p_deleted_count || ' old successful messages');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_CLEANUP_OLD_MESSAGES;
/

-- Procedure to reset failed messages for retry
CREATE OR REPLACE PROCEDURE SP_RESET_FOR_RETRY(
    p_message_uuid IN VARCHAR2
) AS
BEGIN
    UPDATE LOAD_PROFILE_INBOUND
    SET STATUS = 'RECEIVED',
        ATTEMPT_COUNT = 0,
        LAST_ATTEMPT_TIMESTAMP = NULL,
        LAST_ERROR_MESSAGE = NULL
    WHERE MESSAGE_UUID = p_message_uuid
    AND STATUS IN ('FAILED_RETRY', 'FAILED_DLQ');
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Message not found or not in failed status');
    END IF;
    
    -- Add audit entry
    INSERT INTO LOAD_PROFILE_AUDIT (MESSAGE_UUID, ACTION, NEW_STATUS, ACTION_BY, DETAILS)
    VALUES (p_message_uuid, 'MANUAL_RESET', 'RECEIVED', USER, 'Reset for retry processing');
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Message ' || p_message_uuid || ' reset for retry');
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_RESET_FOR_RETRY;
/

-- =============================================================================
-- Grants for Application User (adjust as needed)
-- =============================================================================
-- GRANT SELECT, INSERT, UPDATE ON LOAD_PROFILE_INBOUND TO APP_USER;
-- GRANT SELECT ON V_LOAD_PROFILE_STATS TO MONITOR_USER;
-- GRANT SELECT ON V_LOAD_PROFILE_ATTENTION TO MONITOR_USER;
-- GRANT SELECT ON V_LOAD_PROFILE_DAILY_SUMMARY TO MONITOR_USER;

-- =============================================================================
-- Verification Queries
-- =============================================================================
-- Check table creation
SELECT table_name, num_rows, last_analyzed 
FROM user_tables 
WHERE table_name LIKE 'LOAD_PROFILE%';

-- Check indexes
SELECT index_name, table_name, uniqueness, status 
FROM user_indexes 
WHERE table_name LIKE 'LOAD_PROFILE%';

-- Check views
SELECT view_name, text_length 
FROM user_views 
WHERE view_name LIKE '%LOAD_PROFILE%';

-- Check procedures
SELECT object_name, object_type, status 
FROM user_objects 
WHERE object_type IN ('PROCEDURE', 'FUNCTION') 
AND object_name LIKE '%LOAD_PROFILE%';

-- =============================================================================
-- End of Script
-- =============================================================================
