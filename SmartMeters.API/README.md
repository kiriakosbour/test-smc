# Load Profile Push API - HEDNO Integration

## Critical System for Load Profile Data Push to SAP

This is a **CRITICAL** production system for pushing load profile data from MDMS to SAP using SOAP web services with Exactly Once (EO) delivery guarantee.

## Project Folder Structure

```
load-profile-push-api/
│
├── pom.xml                                         # Maven configuration
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── hedno/
│   │   │           └── integration/
│   │   │               ├── entity/                 # Database entities
│   │   │               │   └── LoadProfileInbound.java
│   │   │               │
│   │   │               ├── dao/                    # Data Access Objects
│   │   │               │   └── LoadProfileInboundDAO.java
│   │   │               │
│   │   │               ├── soap/
│   │   │               │   ├── model/              # SOAP/XML model classes
│   │   │               │   │   └── UtilitiesTimeSeriesERPItemBulkNotification.java
│   │   │               │   │
│   │   │               │   └── generated/          # JAXB generated from WSDL
│   │   │               │       └── (auto-generated classes)
│   │   │               │
│   │   │               ├── service/                # Business services
│   │   │               │   ├── XMLBuilderService.java
│   │   │               │   └── SoapClientService.java
│   │   │               │
│   │   │               ├── processor/              # Message processors
│   │   │               │   ├── LoadProfileProcessor.java
│   │   │               │   └── LoadProfileDataExtractor.java
│   │   │               │
│   │   │               ├── servlet/                # HTTP endpoints (optional)
│   │   │               │   ├── InboundReceiver.java
│   │   │               │   └── MonitoringServlet.java
│   │   │               │
│   │   │               ├── util/                   # Utility classes
│   │   │               │   ├── ConfigurationManager.java
│   │   │               │   └── XMLValidator.java
│   │   │               │
│   │   │               └── exception/              # Custom exceptions
│   │   │                   ├── ProcessingException.java
│   │   │                   └── ValidationException.java
│   │   │
│   │   ├── resources/
│   │   │   ├── wsdl/                              # WSDL files
│   │   │   │   └── UtilitiesTimeSeriesERPItemBulkNotification_OutService.wsdl
│   │   │   │
│   │   │   ├── xsd/                               # XML schemas
│   │   │   │   └── (schema files if separate from WSDL)
│   │   │   │
│   │   │   ├── config/                            # Configuration files
│   │   │   │   ├── application.properties
│   │   │   │   └── sap-endpoint.properties
│   │   │   │
│   │   │   ├── META-INF/
│   │   │   │   ├── ejb-jar.xml                    # EJB deployment descriptor
│   │   │   │   └── persistence.xml                # JPA configuration (if using JPA)
│   │   │   │
│   │   │   └── logback.xml                        # Logging configuration
│   │   │
│   │   └── webapp/
│   │       ├── WEB-INF/
│   │       │   ├── web.xml                        # Web deployment descriptor
│   │       │   ├── weblogic.xml                   # WebLogic specific config
│   │       │   └── lib/                           # Additional JARs (if needed)
│   │       │
│   │       ├── admin/                             # Admin console pages
│   │       │   ├── dashboard.jsp
│   │       │   ├── messages.jsp
│   │       │   └── config.jsp
│   │       │
│   │       ├── monitor/                           # Monitoring pages
│   │       │   ├── status.jsp
│   │       │   └── statistics.jsp
│   │       │
│   │       ├── error/                             # Error pages
│   │       │   ├── 404.html
│   │       │   ├── 500.html
│   │       │   └── error.html
│   │       │
│   │       └── index.html                         # Welcome page
│   │
│   └── test/
│       ├── java/
│       │   └── com/
│       │       └── hedno/
│       │           └── integration/
│       │               ├── dao/
│       │               │   └── LoadProfileInboundDAOTest.java
│       │               │
│       │               ├── service/
│       │               │   ├── XMLBuilderServiceTest.java
│       │               │   └── SoapClientServiceTest.java
│       │               │
│       │               ├── processor/
│       │               │   └── LoadProfileProcessorTest.java
│       │               │
│       │               └── integration/            # Integration tests
│       │                   └── EndToEndTest.java
│       │
│       └── resources/
│           ├── test-data/                         # Test XML files
│           │   ├── valid-message.xml
│           │   ├── invalid-message.xml
│           │   └── sample-response.xml
│           │
│           └── test.properties                    # Test configuration
│
├── database/
│   ├── setup.sql                                  # Database creation script
│   ├── cleanup.sql                                # Cleanup script
│   ├── test-data.sql                             # Test data insertion
│   └── migration/                                # Database migration scripts
│       ├── V1__initial_schema.sql
│       └── V2__add_indexes.sql
│
├── docs/
│   ├── architecture.md                           # System architecture
│   ├── deployment-guide.md                       # Deployment instructions
│   ├── troubleshooting.md                        # Common issues
│   ├── api-documentation.md                      # API documentation
│   └── monitoring-guide.md                       # Monitoring setup
│
├── scripts/
│   ├── deploy.sh                                 # Deployment script
│   ├── start-processor.sh                        # Start processor
│   ├── stop-processor.sh                         # Stop processor
│   └── check-status.sh                          # Check system status
│
├── config/
│   ├── weblogic/
│   │   ├── config.xml                           # WebLogic domain config
│   │   ├── jdbc-LoadProfileDB.xml               # DataSource configuration
│   │   └── work-manager.xml                     # Work manager config
│   │
│   └── production/
│       ├── application.properties               # Production properties
│       └── logback-production.xml              # Production logging
│
├── lib/                                         # External dependencies (if not in Maven)
│   └── (any proprietary JARs)
│
├── target/                                      # Maven build output (generated)
│   ├── classes/
│   ├── generated-sources/
│   ├── test-classes/
│   └── load-profile-push-api.war
│
├── logs/                                        # Application logs (runtime)
│   ├── application.log
│   ├── error.log
│   └── audit.log
│
├── .gitignore
├── README.md
└── LICENSE
```

## Technology Stack

- **Java Version**: 1.8 (Java 8)
- **Application Server**: WebLogic 14.1.1
- **Database**: Oracle 12c or higher
- **Build Tool**: Maven 3.6+
- **Framework**: Java EE (EJB 3.2, Servlet 4.0)
- **XML Processing**: JAXB 2.3.1
- **SOAP**: JAX-WS 2.3.1
- **Logging**: SLF4J + Logback
- **Connection Pool**: HikariCP (fallback) / WebLogic DataSource (primary)

## Key Components

### 1. **Entity Layer** (`com.hedno.integration.entity`)
- `LoadProfileInbound`: Main entity representing load profile messages

### 2. **Data Access Layer** (`com.hedno.integration.dao`)
- `LoadProfileInboundDAO`: Database operations with connection pooling

### 3. **SOAP Model** (`com.hedno.integration.soap.model`)
- `UtilitiesTimeSeriesERPItemBulkNotification`: JAXB-annotated SOAP message structure

### 4. **Service Layer** (`com.hedno.integration.service`)
- `XMLBuilderService`: Constructs and validates XML according to WSDL
- `SoapClientService`: Handles HTTP/SOAP communication with retry logic

### 5. **Processor Layer** (`com.hedno.integration.processor`)
- `LoadProfileProcessor`: Main EJB processor with timer-based execution
- `LoadProfileDataExtractor`: Extracts data from XML payloads

## Database Schema

```sql
LOAD_PROFILE_INBOUND
├── MESSAGE_UUID (VARCHAR2(36), PK)
├── RAW_PAYLOAD (CLOB)
├── STATUS (VARCHAR2(20))
├── RECEIVED_TIMESTAMP (TIMESTAMP)
├── LAST_ATTEMPT_TIMESTAMP (TIMESTAMP)
├── ATTEMPT_COUNT (NUMBER(3))
└── LAST_ERROR_MESSAGE (VARCHAR2(4000))
```

## Build Instructions

```bash
# Clean and build
mvn clean compile

# Run tests
mvn test

# Package WAR
mvn package

# Deploy to WebLogic
mvn weblogic:deploy
```

## Configuration

### Application Properties
```properties
# SAP Endpoint Configuration
sap.endpoint.host=sapd2ojas67.sapservers.local
sap.endpoint.port=56700
sap.endpoint.path=/XISOAPAdapter/MessageServlet
sap.use.https=true
sap.username=SAP_USER
sap.password=SAP_PASSWORD

# Processing Configuration
processor.interval.ms=60000
processor.batch.size=10
processor.max.retries=3
processor.max.profiles.per.message=10

# Database Configuration (for HikariCP fallback)
db.url=jdbc:oracle:thin:@localhost:1521:XE
db.username=LOAD_PROFILE
db.password=password
```

### WebLogic DataSource

Configure JNDI DataSource: `jdbc/LoadProfileDB`

## Deployment

1. **Database Setup**
   ```bash
   sqlplus sys/password@XE as sysdba
   @database/setup.sql
   ```

2. **Build Application**
   ```bash
   mvn clean package
   ```

3. **Deploy to WebLogic**
   - Via Admin Console: Deployments → Install → Select WAR
   - Via Maven: `mvn weblogic:deploy`
   - Via WLST: `scripts/deploy.sh`

4. **Verify Deployment**
   - Check Admin Console: http://localhost:7001/console
   - Application URL: http://localhost:7001/load-profile-api
   - Monitor logs: `tail -f logs/load-profile-api.log`

## Monitoring

### Health Checks
- Status endpoint: `/monitor/status`
- Statistics: `/monitor/statistics`
- Admin dashboard: `/admin/dashboard.jsp`

### Database Monitoring Views
- `V_LOAD_PROFILE_STATS`: Processing statistics
- `V_LOAD_PROFILE_ATTENTION`: Messages requiring attention
- `V_LOAD_PROFILE_DAILY_SUMMARY`: Daily processing summary

## Error Handling

### Processing States
1. **RECEIVED**: New message awaiting processing
2. **PROCESSING**: Currently being processed
3. **SENT_OK**: Successfully sent to SAP
4. **FAILED_RETRY**: Failed but will retry
5. **FAILED_DLQ**: Moved to Dead Letter Queue

### Retry Logic
- Max retries: 3 (configurable)
- Retry delay: 5 seconds with progressive backoff
- HTTP 4xx/5xx triggers retry
- Exactly Once guarantee via UUID in URL

## Security Considerations

1. **Authentication**: Basic Auth for SAP endpoint
2. **HTTPS**: Enabled by default for SAP communication
3. **Access Control**: Role-based (admin, monitor)
4. **Input Validation**: WSDL-based XML validation
5. **Audit Trail**: All actions logged in LOAD_PROFILE_AUDIT table

## Troubleshooting

### Common Issues

1. **Connection Timeout**
   - Check network connectivity
   - Verify SAP endpoint URL
   - Increase timeout values

2. **XML Validation Errors**
   - Verify against WSDL
   - Check UUID format (36 characters)
   - Validate sender/recipient IDs

3. **Database Connection Issues**
   - Verify JNDI configuration
   - Check Oracle listener status
   - Review connection pool settings

### Log Locations
- Application: `/opt/weblogic/domains/load-profile/logs/`
- WebLogic: `/opt/weblogic/domains/load-profile/servers/AdminServer/logs/`
- Database: Check Oracle alert log

## Support

For issues, contact:
- Development Team: dev-team@hedno.gr
- Operations: ops@hedno.gr
- Emergency: +30-XXX-XXXX

## License

Proprietary - HEDNO © 2025
