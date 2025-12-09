package com.hedno.integration.controller;

import com.hedno.integration.service.MdmImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * MDM Push Receiver Endpoint v3.0
 * 
 * REST API for receiving ZFA MDM push messages and querying processed data.
 * 
 * URL Convention:
 *   Base path: /profiles
 *   Push data: POST /api/profiles/profiles  (or /api/profiles/profiles/{operation})
 *   Debug:     GET  /api/profiles/debug/{logId}
 * 
 * Architecture per Architect's design:
 * - SMC_MDM_SCCURVES_HD: Master/header table (renamed from DEBUG_LOG)
 * - SMC_MDM_SCCURVES: Curve data with HD_LOG_ID FK
 * - Multi-source support: ZFA, ITRON
 * - Multi-type support: MEASURE, ALARM, EVENT
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
@Path("")
@Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML})
@Produces(MediaType.APPLICATION_JSON)
public class LoadProfileReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileReceiverEndpoint.class);
    
    private MdmImportService mdmImportService;

    @PostConstruct
    public void init() {
        this.mdmImportService = new MdmImportService();
        log.info("LoadProfileReceiverEndpoint v3.0 initialized - HD master table architecture");
    }

    // ==========================================================================
    // Health & Status Endpoints
    // ==========================================================================
    
    /**
     * Simple ping endpoint for health checks
     * URL: GET /api/profiles/ping
     */
    @GET
    @Path("/ping")
    @Produces(MediaType.TEXT_PLAIN)
    public Response ping() {
        log.info("Ping called");
        return Response.ok("pong").build();
    }
    
    /**
     * Health check with version info
     * URL: GET /api/profiles/health
     */
    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response health() {
        String json = String.format(
            "{\"status\": \"UP\", \"version\": \"3.0.0\", " +
            "\"hdTable\": \"SMC_MDM_SCCURVES_HD\", " +
            "\"curvesTable\": \"SMC_MDM_SCCURVES\", " +
            "\"supportedSources\": [\"ZFA\", \"ITRON\"], " +
            "\"supportedTypes\": [\"MEASURE\", \"ALARM\", \"EVENT\"]}"
        );
        return Response.ok(json).build();
    }

    // ==========================================================================
    // Main Push Endpoints
    // ==========================================================================
    
    /**
     * Main endpoint that receives, parses, and saves ZFA MDM XML.
     * URL: POST /api/profiles/profiles
     * 
     * Flow:
     * 1. Creates header record in SMC_MDM_SCCURVES_HD (PENDING)
     * 2. Parses XML and extracts profiles
     * 3. Transforms vertical intervals to horizontal Q1-Q96 columns
     * 4. Saves to SMC_MDM_SCCURVES with HD_LOG_ID reference
     * 5. Updates header to SUCCESS/ERROR
     * 
     * @param xmlBody Raw SOAP/XML payload
     * @return Response with HD_LOG_ID
     */
    @POST
    @Path("/profiles")
    @Produces(MediaType.APPLICATION_JSON)
    public Response receiveProfileData(String xmlBody) {
        return receiveProfileDataWithOperation(null, xmlBody);
    }
    
    /**
     * Push endpoint with WSDL operation tracking
     * URL: POST /api/profiles/profiles/{operation}
     * 
     * @param operation WSDL operation name
     * @param xmlBody Raw SOAP/XML payload
     * @return Response with HD_LOG_ID
     */
    @POST
    @Path("/profiles/{operation}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response receiveProfileDataWithOperation(
            @PathParam("operation") String operation,
            String xmlBody) {
        
        long startTime = System.currentTimeMillis();
        log.info("Received Profile Data - Operation: {}, Size: {} bytes", 
            operation, (xmlBody != null ? xmlBody.length() : 0));

        if (xmlBody == null || xmlBody.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity("{\"error\": \"Empty request body\"}")
                .build();
        }

        try {
            // Process the XML - returns HD_LOG_ID
            long hdLogId = mdmImportService.processZfaMeasurement(
                xmlBody, 
                "/api/profiles/profiles" + (operation != null ? "/" + operation : ""),
                operation
            );
            
            long duration = System.currentTimeMillis() - startTime;
            
            String response = String.format(
                "{\"status\": \"OK\", \"hdLogId\": %d, \"processingTimeMs\": %d}",
                hdLogId, duration
            );
            
            log.info("Profile Data processing completed - HD_LOG_ID: {}, Duration: {}ms", hdLogId, duration);
            return Response.ok(response).build();
            
        } catch (Exception e) {
            log.error("Profile Data processing failed", e);
            
            String errorResponse = String.format(
                "{\"status\": \"ERROR\", \"error\": \"%s\"}",
                escapeJson(e.getMessage())
            );
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(errorResponse)
                .build();
        }
    }

    // ==========================================================================
    // Query Endpoints
    // ==========================================================================
    
    /**
     * Get debug/header record by LOG_ID
     * URL: GET /api/profiles/debug/{logId}
     * 
     * Returns header information including status and processing summary.
     * Note: Field renamed from errorMsg to statusMsg per architect's request.
     * 
     * @param logId The HD_LOG_ID (was DEBUG_LOG_ID in v2)
     * @return Header record with processing summary
     */
    @GET
    @Path("/debug/{logId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDebugLog(@PathParam("logId") long logId) {
        try {
            Map<String, Object> header = mdmImportService.getHeaderById(logId);
            
            if (header == null) {
                return Response.status(Response.Status.NOT_FOUND)
                    .entity("{\"error\": \"Debug log not found\", \"logId\": " + logId + "}")
                    .build();
            }
            
            // Get processing summary (curve records)
            List<Map<String, Object>> summary = mdmImportService.getProcessingSummary(logId);
            
            // Build response JSON
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"logId\": ").append(header.get("logId")).append(",");
            json.append("\"sourceSystem\": \"").append(header.get("sourceSystem")).append("\",");
            json.append("\"sourceType\": \"").append(header.get("sourceType")).append("\",");
            json.append("\"messageUuid\": \"").append(nullSafe(header.get("messageUuid"))).append("\",");
            json.append("\"status\": \"").append(header.get("status")).append("\",");
            json.append("\"statusMsg\": ").append(jsonString(header.get("statusMsg"))).append(",");
            json.append("\"recordsProcessed\": ").append(header.get("recordsProcessed")).append(",");
            json.append("\"receivedAt\": \"").append(header.get("receivedAt")).append("\",");
            json.append("\"processingSummary\": [");
            
            for (int i = 0; i < summary.size(); i++) {
                Map<String, Object> row = summary.get(i);
                if (i > 0) json.append(",");
                json.append("{");
                json.append("\"podId\": \"").append(row.get("podId")).append("\",");
                json.append("\"supplyNum\": \"").append(row.get("supplyNum")).append("\",");
                json.append("\"dataClass\": \"").append(row.get("dataClass")).append("\",");
                json.append("\"dateRead\": \"").append(row.get("dateRead")).append("\",");
                json.append("\"sectionUuid\": ").append(jsonString(row.get("sectionUuid")));
                json.append("}");
            }
            
            json.append("]}");
            
            return Response.ok(json.toString()).build();
            
        } catch (Exception e) {
            log.error("Error retrieving debug log {}", logId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("{\"error\": \"" + escapeJson(e.getMessage()) + "\"}")
                .build();
        }
    }
    
    /**
     * Get debug record by transaction ID (UUID lookup)
     * URL: GET /api/profiles/debug/uuid/{transactionId}
     * Kept for backward compatibility with v2 API
     * 
     * @param transactionId The MESSAGE_UUID
     * @return Status of the transaction
     */
    @GET
    @Path("/debug/uuid/{transactionId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDebugByUuid(@PathParam("transactionId") String transactionId) {
        try {
            String status = mdmImportService.getLogStatus(transactionId);
            return Response.ok(
                "{\"transactionId\": \"" + transactionId + "\", \"status\": \"" + status + "\"}"
            ).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity("{\"error\": \"" + escapeJson(e.getMessage()) + "\"}")
                .build();
        }
    }

    // ==========================================================================
    // Helper Methods
    // ==========================================================================
    
    private String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("\n", "\\n")
                   .replace("\r", "\\r")
                   .replace("\t", "\\t");
    }
    
    private String nullSafe(Object value) {
        return value != null ? value.toString() : "";
    }
    
    private String jsonString(Object value) {
        if (value == null) return "null";
        return "\"" + escapeJson(value.toString()) + "\"";
    }
}
