# MDM Push Integration v3.0 - Java Changes

## Overview

This package contains the updated Java classes implementing the architect's v3 design for the MDM ZFA Push integration.

## Key Changes from v2

### Table/Field Renaming
| Old Name | New Name | Description |
|----------|----------|-------------|
| `SMC_MDM_DEBUG_LOG` | `SMC_MDM_SCCURVES_HD` | Master/Header table (not just debug) |
| `DEBUG_LOG_ID` | `HD_LOG_ID` | FK in curves table |
| `ERROR_MSG` | `STATUS_MSG` | Empty unless error (κενό εκτός αν πάει κάτι στραβά) |

### New Fields
| Table | Field | Purpose |
|-------|-------|---------|
| `SMC_MDM_SCCURVES_HD` | `SOURCE_SYSTEM` | ZFA or ITRON |
| `SMC_MDM_SCCURVES_HD` | `SOURCE_TYPE` | MEASURE, ALARM, EVENT |
| `SMC_MDM_SCCURVES_HD` | `FILE_ID` | ITRON file identifier |
| `SMC_MDM_SCCURVES_HD` | `FILE_NAME` | ITRON SFTP filename |
| `SMC_MDM_SCCURVES` | `SECTION_UUID` | Inner XML UUID per channel |
| `SMC_MDM_SCCURVES` | `DT_CREATE` | Record creation timestamp |
| `SMC_MDM_SCCURVES` | `DT_UPDATE` | Last update timestamp |

## Files to Replace

Replace these files in your existing `src/main/java/com/hedno/integration/` directory:

```
service/
  └── MdmImportService.java         ← REPLACE

controller/
  └── LoadProfileReceiverEndpoint.java  ← REPLACE

dto/
  └── HeaderLogResponse.java        ← NEW (add this)

processor/
  ├── LoadProfileData.java          ← REPLACE
  └── LoadProfileDataExtractor.java ← REPLACE
```

## Database DDL

The DDL file is in `database/mdm_v3_ddl.sql`. Run this AFTER backing up existing tables.

**If tables already exist with data:**
```sql
-- Rename existing table
ALTER TABLE SMC_MDM_DEBUG_LOG RENAME TO SMC_MDM_SCCURVES_HD;

-- Add new columns
ALTER TABLE SMC_MDM_SCCURVES_HD ADD (
    SOURCE_SYSTEM VARCHAR2(20) DEFAULT 'ZFA',
    SOURCE_TYPE VARCHAR2(20) DEFAULT 'MEASURE',
    FILE_ID VARCHAR2(100),
    FILE_NAME VARCHAR2(255)
);

-- Rename column
ALTER TABLE SMC_MDM_SCCURVES_HD RENAME COLUMN ERROR_MSG TO STATUS_MSG;

-- Add constraints
ALTER TABLE SMC_MDM_SCCURVES_HD ADD CONSTRAINT CK_HD_SOURCE_SYSTEM 
    CHECK (SOURCE_SYSTEM IN ('ZFA', 'ITRON'));
ALTER TABLE SMC_MDM_SCCURVES_HD ADD CONSTRAINT CK_HD_SOURCE_TYPE 
    CHECK (SOURCE_TYPE IN ('MEASURE', 'ALARM', 'EVENT'));

-- Curves table updates
ALTER TABLE SMC_MDM_SCCURVES RENAME COLUMN DEBUG_LOG_ID TO HD_LOG_ID;
ALTER TABLE SMC_MDM_SCCURVES ADD (
    SECTION_UUID VARCHAR2(64),
    DT_CREATE TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    DT_UPDATE TIMESTAMP(6) DEFAULT SYSTIMESTAMP
);
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/profiles/profiles` | Main ZFA push endpoint |
| POST | `/api/profiles/profiles/{operation}` | Push with WSDL operation tracking |
| GET | `/api/profiles/debug/{logId}` | Get debug/header by LOG_ID |
| GET | `/api/profiles/debug/uuid/{transactionId}` | Get status by MESSAGE_UUID |
| GET | `/api/profiles/health` | Health check with version |
| GET | `/api/profiles/ping` | Simple ping (returns "pong") |

## Build & Deploy

### Maven Profiles

| Profile | Command | JNDI Datasource | Use Case |
|---------|---------|-----------------|----------|
| development | `mvn clean package` | java:comp/env/jdbc/LoadProfileDB | Local development |
| devWeblogic | `mvn clean package -P devWeblogic` | jdbc/artemis_smc | Dev WebLogic server |
| production | `mvn clean package -P production` | jdbc/artemis_smc | Production deployment |
| qa | `mvn clean package -P qa` | jdbc/artemis_smc | QA environment |

### Configuration

The `application.properties` file (in `src/main/resources/config/`) uses Maven resource filtering. 
Properties are replaced at build time based on the active profile.

Key properties:
- `datasource.jndi.name` - JNDI name for WebLogic DataSource
- `jdbc.url` / `jdbc.username` / `jdbc.password` - Direct JDBC (development fallback)
- `app.environment` - Environment identifier (dev, devWeblogic, prod, qa)

## Response Format

```json
{
  "status": "OK",
  "hdLogId": 123,
  "processingTimeMs": 45
}
```

## Key Design Decisions

1. **HD as Master**: The `SMC_MDM_SCCURVES_HD` table is now the master table, not just debug logging
2. **Multi-source**: Ready for ITRON integration with `SOURCE_SYSTEM` field
3. **Multi-type**: Supports MEASURE, ALARM, EVENT via `SOURCE_TYPE`
4. **Section tracking**: `SECTION_UUID` links curve rows to inner XML channels
5. **Timezone conversion**: UTC to Europe/Athens for DATE_READ and Q-index calculation
6. **Duplicates allowed**: No unique constraint - downstream ARTEMIS unifies

## Version History

- **v1.0**: 3-table design (DEBUG_LOG → DATA_HD → SCCURVES)
- **v2.0**: 2-table design (DEBUG_LOG → SCCURVES)
- **v3.0**: Architect's design (SCCURVES_HD → SCCURVES + multi-source support)
