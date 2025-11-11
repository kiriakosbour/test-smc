#!/bin/bash

# Oracle Database setup for WildFly testing
ORACLE_HOME=/opt/oracle/instantclient
DB_HOST=localhost
DB_PORT=1521
DB_SID=XE
DB_USER=LOAD_PROFILE
DB_PASS=password

echo "🗄️ Setting up Oracle database for SmartMeters API..."

# Run the setup SQL
sqlplus sys/oracle@$DB_HOST:$DB_PORT/$DB_SID as sysdba <<EOF
-- Create user if not exists
CREATE USER $DB_USER IDENTIFIED BY "$DB_PASS"
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO $DB_USER;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE PROCEDURE, CREATE TRIGGER, CREATE VIEW TO $DB_USER;

-- Connect as LOAD_PROFILE user
CONNECT $DB_USER/$DB_PASS@$DB_HOST:$DB_PORT/$DB_SID;

-- Run the main setup script
@database/smc-script.sql

-- Insert test data
INSERT INTO SMC_PUSH_RELEVANT_CHANNELS (CHANNEL_ID, IS_RELEVANT, DESCRIPTION)
VALUES ('TEST_CHANNEL_001', 1, 'Test channel for WildFly');
COMMIT;

EXIT;
EOF

echo "✅ Database setup complete!"