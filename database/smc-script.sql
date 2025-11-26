-- =============================================================================
-- Load Profile Push API - COMPLETE & ROBUST Database Setup Script (v4.1)
--
-- This script contains:
--   1. The original schema for the "Send Queue" (SMC_LOAD_PROFILE_INBOUND).
--   2. The NEW schema for the "Batching Queue" (SMC_ORDER_PACKAGES).
--
-- UPDATES v4.1:
--   - Added UNIQUE CONSTRAINT on SMC_ORDER_ITEMS(PROFIL_BLOC_ID)
--   - Updated SP_SMC_ADD_ORDER_ITEM to be idempotent (handle duplicates)
--
-- This script is idempotent and will safely drop all objects before
-- recreating them.
-- =============================================================================

-- =============================================================================
-- Part 1: Safely Drop All Existing Objects (Ignores "does not exist" errors)
-- =============================================================================

PROMPT Dropping existing objects (if they exist)...

-- Drop New Batching Procedures
BEGIN
   EXECUTE IMMEDIATE 'DROP PROCEDURE SP_SMC_ADD_ORDER_ITEM';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP PROCEDURE SP_SMC_RESET_FAILED_PACKAGE';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

-- Drop Original Send Procedures
BEGIN
   EXECUTE IMMEDIATE 'DROP PROCEDURE SP_SMC_CLEANUP_OLD_MESSAGES';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP PROCEDURE SP_SMC_RESET_FOR_RETRY';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -4043 THEN RAISE; END IF;
END;
/

-- Drop New Batching Views
BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW V_SMC_ORDER_PACKAGE_STATS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW V_SMC_ORDER_PACKAGE_DETAILS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

-- Drop Original Send Views
BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW V_SMC_LOAD_PROFILE_STATS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW V_SMC_LOAD_PROFILE_ATTENTION';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP VIEW V_SMC_LOAD_PROFILE_DAILY_SUMMARY';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

-- Drop New Batching Tables (Order is important: child first)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE SMC_ORDER_ITEMS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE SMC_ORDER_PACKAGES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

-- Drop Original Send Tables
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE SMC_LOAD_PROFILE_INBOUND CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE SMC_LOAD_PROFILE_AUDIT CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE SMC_LOAD_PROFILE_CONFIG CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

-- Drop All Sequences
BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_SMC_LOAD_PROFILE_AUDIT';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_SMC_ORDER_PACKAGES';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

BEGIN
   EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_SMC_ORDER_ITEMS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -2289 THEN RAISE; END IF;
END;
/

-- =============================================================================
-- Part 2: Create Send Queue Objects (Original Schema)
-- =============================================================================

PROMPT Creating Table: SMC_LOAD_PROFILE_INBOUND (Send Queue)...

CREATE TABLE SMC_LOAD_PROFILE_INBOUND (
    MESSAGE_UUID VARCHAR2(36) NOT NULL,
    RAW_PAYLOAD CLOB NOT NULL,
    STATUS VARCHAR2(20) NOT NULL,
    RECEIVED_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    PROCESSING_START_TIME TIMESTAMP,
    PROCESSING_END_TIME TIMESTAMP,
    LAST_MANUAL_RETRY_TIME TIMESTAMP,
    LAST_ATTEMPT_TIMESTAMP TIMESTAMP,
    LAST_HTTP_STATUS_CODE NUMBER(5) DEFAULT 0,
    LAST_RESPONSE_MESSAGE VARCHAR2(500),
    LAST_ERROR_MESSAGE VARCHAR2(4000),
    ATTEMPT_COUNT NUMBER(3) DEFAULT 0 NOT NULL,
    ORIGINAL_MESSAGE_ID VARCHAR2(36),
    MANUAL_RETRY_COUNT NUMBER(3) DEFAULT 0,
    PROCESSED_BY VARCHAR2(100),
    NOTES VARCHAR2(2000),
    
    CONSTRAINT PK_SMC_LOAD_PROFILE_INBOUND PRIMARY KEY (MESSAGE_UUID),
    CONSTRAINT CHK_SMC_STATUS CHECK (STATUS IN (
        'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED',
        'RECEIVED', 'SENT_OK', 'FAILED_RETRY', 'FAILED_DLQ'
    )),
    CONSTRAINT CHK_SMC_ATTEMPT_COUNT CHECK (ATTEMPT_COUNT >= 0)
) TABLESPACE USERS;

PROMPT Creating Indexes for SMC_LOAD_PROFILE_INBOUND...

CREATE INDEX IDX_SMC_LOAD_PROFILE_STATUS 
ON SMC_LOAD_PROFILE_INBOUND (STATUS, RECEIVED_TIMESTAMP)
TABLESPACE USERS;

CREATE INDEX IDX_SMC_LOAD_PROFILE_RECEIVED
ON SMC_LOAD_PROFILE_INBOUND (RECEIVED_TIMESTAMP DESC)
TABLESPACE USERS;

-- =============================================================================
-- Part 3: Create Batching & Order Tables (New Schema)
-- =============================================================================

PROMPT Creating Table: SMC_ORDER_PACKAGES (Batching Queue)...

CREATE SEQUENCE SEQ_SMC_ORDER_PACKAGES 
    START WITH 1 
    INCREMENT BY 1 
    NOMAXVALUE 
    NOCYCLE 
    CACHE 20;

CREATE TABLE SMC_ORDER_PACKAGES (
    PACKAGE_ID NUMBER DEFAULT SEQ_SMC_ORDER_PACKAGES.NEXTVAL NOT NULL,
    STATUS VARCHAR2(20) NOT NULL,
    CREATED_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    CHANNEL_ID VARCHAR2(100),
    
    CONSTRAINT PK_SMC_ORDER_PACKAGES PRIMARY KEY (PACKAGE_ID),
    CONSTRAINT CHK_SMC_PACKAGE_STATUS CHECK (STATUS IN ('OPEN', 'PROCESSING', 'FAILED', 'COMPLETED'))
) TABLESPACE USERS;

COMMENT ON TABLE SMC_ORDER_PACKAGES IS 'Collects order items into batches (v4.0)';

PROMPT Creating Table: SMC_ORDER_ITEMS (Batching Items)...

CREATE SEQUENCE SEQ_SMC_ORDER_ITEMS 
    START WITH 1 
    INCREMENT BY 1 
    NOMAXVALUE 
    NOCYCLE 
    CACHE 20;

CREATE TABLE SMC_ORDER_ITEMS (
    ITEM_ID NUMBER DEFAULT SEQ_SMC_ORDER_ITEMS.NEXTVAL NOT NULL,
    PACKAGE_ID NUMBER,
    PROFIL_BLOC_ID VARCHAR2(100) NOT NULL, -- Reference to the source data
    DATA_TYPE VARCHAR2(50) NOT NULL, 
    OBIS_CODE VARCHAR2(50),
    POD_ID VARCHAR2(100),
    STATUS VARCHAR2(20) DEFAULT 'PENDING' NOT NULL,
    CREATED_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    RAW_XML CLOB,
    
    CONSTRAINT PK_SMC_ORDER_ITEMS PRIMARY KEY (ITEM_ID),
    CONSTRAINT FK_SMC_ORDER_ITEM_PACKAGE FOREIGN KEY (PACKAGE_ID)
        REFERENCES SMC_ORDER_PACKAGES(PACKAGE_ID) ON DELETE SET NULL,
    CONSTRAINT CHK_SMC_ITEM_STATUS CHECK (STATUS IN ('PENDING', 'PROCESSED')),
    -- FIX v4.1: Ensure each ProfilBloc is only processed once
    CONSTRAINT UK_SMC_PROFIL_BLOC UNIQUE (PROFIL_BLOC_ID)
) TABLESPACE USERS;

COMMENT ON TABLE SMC_ORDER_ITEMS IS 'Individual data points from PROFIL_BLOC to be batched (v4.0)';

PROMPT Creating Indexes for Batching Tables...

CREATE INDEX IDX_SMC_ORDER_PACKAGE_STATUS 
ON SMC_ORDER_PACKAGES (STATUS, CREATED_TIMESTAMP)
TABLESPACE USERS;

CREATE INDEX IDX_SMC_ORDER_ITEM_PACKAGE 
ON SMC_ORDER_ITEMS (PACKAGE_ID)
TABLESPACE USERS;

-- =============================================================================
-- Part 4: Create Audit & Config Tables
-- =============================================================================

PROMPT Creating Audit Table and Sequence...

CREATE SEQUENCE SEQ_SMC_LOAD_PROFILE_AUDIT 
    START WITH 1 
    INCREMENT BY 1 
    NOMAXVALUE 
    NOCYCLE 
    CACHE 20;

CREATE TABLE SMC_LOAD_PROFILE_AUDIT (
    AUDIT_ID NUMBER DEFAULT SEQ_SMC_LOAD_PROFILE_AUDIT.NEXTVAL NOT NULL,
    MESSAGE_UUID VARCHAR2(36) NOT NULL,
    ACTION VARCHAR2(50) NOT NULL,
    OLD_STATUS VARCHAR2(20),
    NEW_STATUS VARCHAR2(20),
    ACTION_TIMESTAMP TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    ACTION_BY VARCHAR2(100),
    DETAILS VARCHAR2(4000),
    
    CONSTRAINT PK_SMC_LOAD_PROFILE_AUDIT PRIMARY KEY (AUDIT_ID)
) TABLESPACE USERS;

CREATE INDEX IDX_SMC_AUDIT_MESSAGE_UUID ON SMC_LOAD_PROFILE_AUDIT (MESSAGE_UUID);
CREATE INDEX IDX_SMC_AUDIT_TIMESTAMP ON SMC_LOAD_PROFILE_AUDIT (ACTION_TIMESTAMP DESC);

PROMPT Creating Configuration Table and defaults...

CREATE TABLE SMC_LOAD_PROFILE_CONFIG (
    CONFIG_KEY VARCHAR2(100) NOT NULL,
    CONFIG_VALUE VARCHAR2(500),
    CONFIG_DESC VARCHAR2(1000),
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP,
    UPDATED_BY VARCHAR2(100),
    
    CONSTRAINT PK_SMC_LOAD_PROFILE_CONFIG PRIMARY KEY (CONFIG_KEY)
) TABLESPACE USERS;

-- Insert Configuration Defaults
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('PROCESSOR_INTERVAL_MS', '60000', 'Processing interval in milliseconds (Send Queue)');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('BATCH_SIZE', '10', 'Number of messages to process in each batch (Send Queue)');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('MAX_PROFILES_PER_MESSAGE', '10', 'Maximum profiles per SOAP message');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('SAP_ENDPOINT_HOST', 'sapd2ojas67.sapservers.local', 'SAP endpoint hostname');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('SAP_ENDPOINT_PORT', '56700', 'SAP endpoint port');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('SAP_ENDPOINT_PATH', '/XISOAPAdapter/MessageServlet', 'SAP endpoint path');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('USE_HTTPS', 'true', 'Use HTTPS for SAP communication');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('CONNECT_TIMEOUT_MS', '30000', 'Connection timeout in milliseconds');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('READ_TIMEOUT_MS', '60000', 'Read timeout in milliseconds');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('ORDER_PROCESSOR_INTERVAL_MS', '300000', 'Processing interval in milliseconds (Batching Queue, e.g., 5 mins)');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('ORDER_PROCESSOR_DELAY_MINUTES', '60', 'Max age of a package before it is sent (e.g., 60 mins)');
INSERT INTO SMC_LOAD_PROFILE_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_DESC) VALUES ('ORDER_PROCESSOR_MAX_SIZE', '100', 'Max number of items in a package before it is sent (e.g., 100)');

COMMIT;

-- =============================================================================
-- Part 5: Create Monitoring Views
-- =============================================================================

PROMPT Creating View: V_SMC_LOAD_PROFILE_STATS...
CREATE OR REPLACE VIEW V_SMC_LOAD_PROFILE_STATS AS
SELECT 
    STATUS,
    COUNT(*) AS MESSAGE_COUNT,
    MIN(RECEIVED_TIMESTAMP) AS OLDEST_MESSAGE,
    MAX(RECEIVED_TIMESTAMP) AS NEWEST_MESSAGE,
    AVG(MANUAL_RETRY_COUNT) AS AVG_MANUAL_RETRIES
FROM SMC_LOAD_PROFILE_INBOUND
GROUP BY STATUS;

PROMPT Creating View: V_SMC_LOAD_PROFILE_ATTENTION...
CREATE OR REPLACE VIEW V_SMC_LOAD_PROFILE_ATTENTION AS
SELECT 
    MESSAGE_UUID,
    STATUS,
    RECEIVED_TIMESTAMP,
    PROCESSING_START_TIME,
    MANUAL_RETRY_COUNT,
    SUBSTR(LAST_ERROR_MESSAGE, 1, 200) AS ERROR_SUMMARY,
    CASE 
        WHEN STATUS = 'FAILED' THEN 'FAILED - MANUAL RETRY'
        WHEN STATUS = 'PROCESSING' AND PROCESSING_START_TIME < SYSTIMESTAMP - INTERVAL '30' MINUTE THEN 'STUCK - PROCESSING'
        ELSE 'PENDING'
    END AS ATTENTION_TYPE
FROM SMC_LOAD_PROFILE_INBOUND
WHERE STATUS IN ('FAILED', 'PROCESSING')
ORDER BY RECEIVED_TIMESTAMP;

PROMPT Creating View: V_SMC_LOAD_PROFILE_DAILY_SUMMARY...
CREATE OR REPLACE VIEW V_SMC_LOAD_PROFILE_DAILY_SUMMARY AS
SELECT 
    TRUNC(RECEIVED_TIMESTAMP) AS PROCESS_DATE,
    COUNT(*) AS TOTAL_MESSAGES,
    SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS SUCCESSFUL,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED,
    SUM(CASE WHEN STATUS IN ('PENDING', 'PROCESSING') THEN 1 ELSE 0 END) AS PENDING_OR_PROCESSING,
    ROUND(AVG(MANUAL_RETRY_COUNT), 2) AS AVG_MANUAL_RETRIES
FROM SMC_LOAD_PROFILE_INBOUND
GROUP BY TRUNC(RECEIVED_TIMESTAMP)
ORDER BY PROCESS_DATE DESC;

PROMPT Creating View: V_SMC_ORDER_PACKAGE_STATS...
CREATE OR REPLACE VIEW V_SMC_ORDER_PACKAGE_STATS AS
SELECT 
    STATUS,
    COUNT(*) AS PACKAGE_COUNT,
    MIN(CREATED_TIMESTAMP) AS OLDEST_PACKAGE,
    MAX(CREATED_TIMESTAMP) AS NEWEST_PACKAGE
FROM SMC_ORDER_PACKAGES
GROUP BY STATUS;

PROMPT Creating View: V_SMC_ORDER_PACKAGE_DETAILS...
CREATE OR REPLACE VIEW V_SMC_ORDER_PACKAGE_DETAILS AS
SELECT
    p.PACKAGE_ID,
    p.STATUS,
    p.CHANNEL_ID,
    p.CREATED_TIMESTAMP,
    (SYSTIMESTAMP - p.CREATED_TIMESTAMP) AS AGE,
    (SELECT COUNT(*) FROM SMC_ORDER_ITEMS i WHERE i.PACKAGE_ID = p.PACKAGE_ID) AS ITEM_COUNT,
    (SELECT COUNT(*) FROM SMC_ORDER_ITEMS i WHERE i.PACKAGE_ID = p.PACKAGE_ID AND i.STATUS = 'PENDING') AS PENDING_ITEMS
FROM SMC_ORDER_PACKAGES p
ORDER BY p.CREATED_TIMESTAMP;

-- =============================================================================
-- Part 6: Stored Procedures
-- =============================================================================

PROMPT Creating Procedure: SP_SMC_CLEANUP_OLD_MESSAGES...
CREATE OR REPLACE PROCEDURE SP_SMC_CLEANUP_OLD_MESSAGES(
    p_days_to_keep IN NUMBER DEFAULT 30,
    p_deleted_count OUT NUMBER
) AS
BEGIN
    DELETE FROM SMC_LOAD_PROFILE_INBOUND
    WHERE STATUS = 'COMPLETED'
    AND RECEIVED_TIMESTAMP < SYSTIMESTAMP - INTERVAL '1' DAY * p_days_to_keep;
    
    p_deleted_count := SQL%ROWCOUNT;
    
    IF p_deleted_count > 0 THEN
        INSERT INTO SMC_LOAD_PROFILE_AUDIT (
            MESSAGE_UUID, ACTION, NEW_STATUS, ACTION_BY, DETAILS
        ) VALUES (
            'CLEANUP-' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISS'),
            'CLEANUP',
            NULL,
            USER,
            'Deleted ' || p_deleted_count || ' COMPLETED messages older than ' || p_days_to_keep || ' days'
        );
    END IF;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SMC_CLEANUP_OLD_MESSAGES;
/

PROMPT Creating Procedure: SP_SMC_RESET_FOR_RETRY...
CREATE OR REPLACE PROCEDURE SP_SMC_RESET_FOR_RETRY(
    p_message_uuid IN VARCHAR2
) AS
    v_old_status VARCHAR2(20);
BEGIN
    SELECT STATUS INTO v_old_status
    FROM SMC_LOAD_PROFILE_INBOUND
    WHERE MESSAGE_UUID = p_message_uuid
    FOR UPDATE;
    
    UPDATE SMC_LOAD_PROFILE_INBOUND
    SET STATUS = 'PENDING',
        PROCESSING_START_TIME = NULL,
        PROCESSING_END_TIME = NULL,
        LAST_HTTP_STATUS_CODE = 0,
        LAST_RESPONSE_MESSAGE = NULL,
        LAST_ERROR_MESSAGE = NULL,
        MANUAL_RETRY_COUNT = MANUAL_RETRY_COUNT + 1,
        LAST_MANUAL_RETRY_TIME = SYSTIMESTAMP
    WHERE MESSAGE_UUID = p_message_uuid
    AND STATUS = 'FAILED';
    
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Message not found or not in FAILED status');
    END IF;
    
    INSERT INTO SMC_LOAD_PROFILE_AUDIT (
        MESSAGE_UUID, ACTION, OLD_STATUS, NEW_STATUS, ACTION_BY, DETAILS
    ) VALUES (
        p_message_uuid, 'MANUAL_RESET', v_old_status, 'PENDING', USER, 'Reset for retry processing'
    );
    
    COMMIT;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20002, 'Message not found: ' || p_message_uuid);
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SMC_RESET_FOR_RETRY;
/

PROMPT Creating Procedure: SP_SMC_ADD_ORDER_ITEM (With Idempotency)...

CREATE OR REPLACE PROCEDURE SP_SMC_ADD_ORDER_ITEM(
    p_channel_id IN VARCHAR2,
    p_profil_bloc_id IN VARCHAR2,
    p_data_type IN VARCHAR2,
    p_obis_code IN VARCHAR2,
    p_pod_id IN VARCHAR2,
    p_raw_xml IN CLOB,
    p_package_id OUT NUMBER
) AS
    v_package_id NUMBER;
BEGIN
    -- Step 1: Try to find an existing 'OPEN' package for this channel
    BEGIN
        SELECT PACKAGE_ID INTO v_package_id
        FROM SMC_ORDER_PACKAGES
        WHERE CHANNEL_ID = p_channel_id
          AND STATUS = 'OPEN'
          AND ROWNUM = 1
        FOR UPDATE;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_package_id := NULL;
    END;

    -- Step 2: If no 'OPEN' package exists, create a new one
    IF v_package_id IS NULL THEN
        INSERT INTO SMC_ORDER_PACKAGES (STATUS, CHANNEL_ID)
        VALUES ('OPEN', p_channel_id)
        RETURNING PACKAGE_ID INTO v_package_id;
    END IF;

    -- Step 3: Insert the new item (Protected by UNIQUE CONSTRAINT)
    BEGIN
        INSERT INTO SMC_ORDER_ITEMS (
            PACKAGE_ID, 
            PROFIL_BLOC_ID, 
            DATA_TYPE, 
            OBIS_CODE, 
            POD_ID, 
            STATUS,
            RAW_XML
        ) VALUES (
            v_package_id,
            p_profil_bloc_id,
            p_data_type,
            p_obis_code,
            p_pod_id,
            'PENDING',
            p_raw_xml
        );
        
        -- If successful, we are done.
        p_package_id := v_package_id;
        
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN
            -- FIX v4.1: IDEMPOTENCY CHECK
            -- If we are here, the PROFIL_BLOC_ID already exists.
            -- Instead of crashing, we gracefully retrieve the existing package ID.
            BEGIN
                SELECT PACKAGE_ID INTO p_package_id
                FROM SMC_ORDER_ITEMS
                WHERE PROFIL_BLOC_ID = p_profil_bloc_id;
                
                -- Optional logging or just silent success
                DBMS_OUTPUT.PUT_LINE('Duplicate ignored for: ' || p_profil_bloc_id);
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   -- Edge case: Item deleted but index not committed? Should not happen.
                   RAISE;
            END;
    END;

EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END SP_SMC_ADD_ORDER_ITEM;
/

PROMPT Creating Procedure: SP_SMC_RESET_FAILED_PACKAGE...
CREATE OR REPLACE PROCEDURE SP_SMC_RESET_FAILED_PACKAGE(
    p_package_id IN NUMBER
) AS
BEGIN
    UPDATE SMC_ORDER_PACKAGES
    SET STATUS = 'OPEN'
    WHERE PACKAGE_ID = p_package_id
      AND STATUS = 'FAILED';
      
    IF SQL%ROWCOUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20003, 'Package not found or not in FAILED status');
    END IF;
    
    UPDATE SMC_ORDER_ITEMS
    SET STATUS = 'PENDING'
    WHERE PACKAGE_ID = p_package_id;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END SP_SMC_RESET_FAILED_PACKAGE;
/

-- =============================================================================
-- Part 7: Triggers
-- =============================================================================

PROMPT Creating Trigger: TRG_SMC_LOAD_PROFILE_AUDIT...
CREATE OR REPLACE TRIGGER TRG_SMC_LOAD_PROFILE_AUDIT
AFTER INSERT OR UPDATE ON SMC_LOAD_PROFILE_INBOUND
FOR EACH ROW
DECLARE
    v_action VARCHAR2(50);
    v_user VARCHAR2(100);
BEGIN
    IF INSERTING THEN
        v_action := 'INSERT';
    ELSIF UPDATING THEN
        v_action := 'UPDATE';
    END IF;
    
    v_user := NVL(SYS_CONTEXT('USERENV', 'OS_USER'), USER);
    
    INSERT INTO SMC_LOAD_PROFILE_AUDIT (
        MESSAGE_UUID,
        ACTION,
        OLD_STATUS,
        NEW_STATUS,
        ACTION_BY,
        DETAILS
    ) VALUES (
        :NEW.MESSAGE_UUID,
        v_action,
        :OLD.STATUS,
        :NEW.STATUS,
        v_user,
        CASE 
            WHEN :NEW.STATUS = 'COMPLETED' THEN 'Successfully sent to SAP'
            WHEN :NEW.STATUS = 'FAILED' THEN 'Failed, awaiting manual retry. Error: ' || SUBSTR(:NEW.LAST_ERROR_MESSAGE, 1, 200)
            WHEN :NEW.STATUS = 'PROCESSING' AND :OLD.STATUS = 'PENDING' THEN 'Processor picked up message'
            ELSE NULL
        END
    );
EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

-- =============================================================================
-- Part 8: NEW Table for "Push Relevant" Channel Configuration
-- =============================================================================

PROMPT Creating Table: SMC_PUSH_RELEVANT_CHANNELS...

CREATE TABLE SMC_PUSH_RELEVANT_CHANNELS (
    CHANNEL_ID VARCHAR2(100) NOT NULL,
    IS_RELEVANT NUMBER(1) DEFAULT 1 NOT NULL,
    DESCRIPTION VARCHAR2(500),
    LAST_UPDATED TIMESTAMP DEFAULT SYSTIMESTAMP,
    
    CONSTRAINT PK_SMC_PUSH_RELEVANT_CHANNELS PRIMARY KEY (CHANNEL_ID),
    CONSTRAINT CHK_SMC_IS_RELEVANT CHECK (IS_RELEVANT IN (0, 1))
) TABLESPACE USERS;

PROMPT Creating Index for SMC_PUSH_RELEVANT_CHANNELS...
CREATE INDEX IDX_SMC_PUSH_RELEVANT_CHANNEL 
ON SMC_PUSH_RELEVANT_CHANNELS (CHANNEL_ID)
TABLESPACE USERS;

-- Insert a default value for testing
INSERT INTO SMC_PUSH_RELEVANT_CHANNELS (CHANNEL_ID, IS_RELEVANT, DESCRIPTION)
VALUES ('DEFAULT_CHANNEL_123', 1, 'Default test channel');
COMMIT;

PROMPT
PROMPT =====================================================
PROMPT Database setup (v4.1 - Fixed) completed successfully!
PROMPT All objects for Batching and Sending are created.
PROMPT =====================================================
PROMPT