# SmartMeters Connector v3.0

HEDNO Smart Meters Integration for Artemis System

## Overview

This application receives and stores smart meter load profile data from MDM sources:
- **ZFA**: Push-based SOAP/XML endpoint
- **ITRON**: SFTP file-based integration

Data is stored in Oracle database tables for direct consumption by the Artemis system.

## Architecture

```
┌─────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│    ZFA      │────>│  SmartMeters         │────>│    Oracle DB    │
│  (SOAP)     │     │  Connector           │     │                 │
└─────────────┘     │                      │     │ SMC_MDM_CURVES  │
                    │  - REST Endpoint     │     │ SMC_MDM_HD      │
┌─────────────┐     │  - SFTP Scheduler    │     └────────┬────────┘
│   ITRON     │────>│  - XML Parser        │              │
│  (SFTP)     │     │  - DB Storage        │              ▼
└─────────────┘     └──────────────────────┘     ┌─────────────────┐
                                                 │    Artemis      │
                                                 │   (Consumer)    │
                                                 └─────────────────┘
```

## Build Instructions

### Maven Profiles

| Profile | Description | JNDI DataSource | Command |
|---------|-------------|-----------------|---------|
| development | Local dev with JDBC | java:comp/env/jdbc/LoadProfileDB | `mvn clean package` |
| devWeblogic | Dev WebLogic | jdbc/artemis_smc | `mvn clean package -P devWeblogic` |
| qa | QA environment | jdbc/artemis_smc | `mvn clean package -P qa` |
| production | Production | jdbc/artemis_smc | `mvn clean package -P production` |

### Build Commands

```bash
# Development (default)
mvn clean package

# Development WebLogic
mvn clean package -P devWeblogic

# QA
mvn clean package -P qa

# Production
mvn clean package -P production

# Run tests
mvn test
```

## Configuration

Configuration is managed via Maven profiles and resource filtering.

### application.properties

Located at `src/main/resources/config/application.properties`

Key properties (replaced at build time):
- `jndi.datasource.name` - JNDI name for database connection
- `db.url`, `db.username`, `db.password` - Direct JDBC (development fallback)
- `sftp.host`, `sftp.port`, `sftp.username`, `sftp.password` - ITRON SFTP
- `processor.interval.ms` - SFTP polling interval
- `processor.batch.size` - Batch size for processing

## API Endpoints

### Push Load Profile Data
```
POST /smartmeters.connector/api/profiles/profiles
Content-Type: application/xml

<UtilitiesTimeSeriesERPItemBulkNotification>
  ...
</UtilitiesTimeSeriesERPItemBulkNotification>
```

### Check Status
```
GET /smartmeters.connector/api/profiles/status/{transactionId}
```

### Get Header Details
```
GET /smartmeters.connector/api/profiles/header/{logId}
```

### Health Check
```
GET /smartmeters.connector/api/profiles/health
```

## Database Schema

### Main Tables

| Table | Description |
|-------|-------------|
| SMC_MDM_SCCURVES_HD | Header/master table for each message |
| SMC_MDM_SCCURVES | Curve data with Q1-Q96 values |
| ITRON_FILE_PROCESS | ITRON file processing tracking |
| ITRON_FILE_READINGS | ITRON readings data |
| ITRON_FILE_ALARMS | ITRON alarms data |
| ITRON_FILE_EVENTS | ITRON events data |

### Horizontal Pivot Design

Curve data uses a horizontal pivot design with Q1-Q100 columns:
- Q1 = 00:00-00:14
- Q2 = 00:15-00:29
- ...
- Q96 = 23:45-23:59
- Q97-Q100 = Reserved for DST transitions

Each Q column has a corresponding S column for status codes.

## Deployment

### WebLogic Deployment

1. Configure JNDI DataSource `jdbc/artemis_smc`
2. Build with appropriate profile: `mvn clean package -P production`
3. Deploy `target/smartmeters.connector.war`

### Database Setup

1. Run `database/01_schema.sql` to create tables
2. Grant appropriate permissions to application user

## Source Systems

### ZFA (Push)
- Receives SOAP/XML via REST endpoint
- Processes immediately
- Stores in SMC_MDM_SCCURVES_HD and SMC_MDM_SCCURVES

### ITRON (SFTP)
- Polls SFTP server on schedule
- Downloads XML files
- Processes readings, alarms, events
- Stores in ITRON_FILE_* tables

## Timezone Handling

- Input: UTC timestamps
- Storage: Greek local time (Europe/Athens)
- Conversion: Automatic UTC -> Greek timezone

## Logging

Logs are written to:
- Console (development)
- `/opt/weblogic/domains/smartmeters/logs/smartmeters-connector.log` (production)

Log levels can be configured in `logback.xml`.

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 3.0 | 2024-12 | Removed SAP integration, added Artemis direct consumption |
| 2.0 | 2024-11 | Added ITRON support, horizontal pivot |
| 1.0 | 2024-10 | Initial ZFA integration |

## Support

For issues or questions, contact the HEDNO Integration Team.
