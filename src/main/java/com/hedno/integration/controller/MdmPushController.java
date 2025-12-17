package com.hedno.integration.controller;

import com.hedno.integration.service.MdmImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * MDM Push REST Controller.
 * Handles incoming ZFA and ITRON data pushes.
 * 
 * Endpoints:
 * - POST /profiles/profiles - Main data push endpoint
 * - GET /profiles/status/{txId} - Check processing status
 * - GET /profiles/health - Health check
 * 
 * Note: Data is stored for Artemis consumption - no SAP integration.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
@Path("")
public class MdmPushController {

    private static final Logger logger = LoggerFactory.getLogger(MdmPushController.class);

    private final MdmImportService importService;

    public MdmPushController() {
        this.importService = new MdmImportService();
    }

    @POST
    @Path("/profilesOpen")
    @Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML, "application/soap+xml"})
    @Produces(MediaType.APPLICATION_XML)
    public Response pushLoadProfileOpen(InputStream xmlStream, @Context HttpServletRequest request) {
    	return pushLoadProfile(xmlStream, request);
    }
    
    /**
     * Main push endpoint for load profile data
     * Accepts XML payload from ZFA or other MDM sources
     */
    @POST
    @Path("/profiles")
    @Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML, "application/soap+xml"})
    @Produces(MediaType.APPLICATION_XML)
    public Response pushLoadProfile(InputStream bodyStream, @Context HttpServletRequest request) {
        String txId = UUID.randomUUID().toString().toUpperCase();
        long startTime = System.currentTimeMillis();

        logger.info("Received load profile push - TxId: {}, ContentType: {}, ContentLength: {}",
            txId, request.getContentType(), request.getContentLength());

        try {
            // Read XML body
            String xmlBody = readRequestBody(bodyStream);

            if (xmlBody == null || xmlBody.trim().isEmpty()) {
                logger.warn("Empty request body received - TxId: {}", txId);
                return buildErrorResponse(txId, "Empty request body", 400);
            }

            logger.debug("Request body length: {} chars", xmlBody.length());

            // Determine source system from content
            String sourceSystem = detectSourceSystem(xmlBody);
            String endpoint = request.getRequestURI();

            // Process the XML
            long hdLogId = importService.processXmlPayload(
                xmlBody, 
                endpoint, 
                "pushLoadProfile",
                sourceSystem,
                MdmImportService.SOURCE_TYPE_MEASURE,
                null, 
                null
            );

            long duration = System.currentTimeMillis() - startTime;
            logger.info("Load profile processed successfully - TxId: {}, HdLogId: {}, Duration: {}ms",
                txId, hdLogId, duration);

            return buildSuccessResponse(txId, hdLogId);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            logger.error("Error processing load profile - TxId: {}, Duration: {}ms", 
                txId, duration, e);
            return buildErrorResponse(txId, e.getMessage(), 500);
        }
    }

    /**
     * Check processing status by transaction ID
     */
    @GET
    @Path("/status/{txId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStatus(@PathParam("txId") String txId) {
        try {
            String status = importService.getLogStatus(txId);
            return Response.ok()
                .entity("{\"txId\":\"" + txId + "\",\"status\":\"" + status + "\"}")
                .build();
        } catch (Exception e) {
            logger.error("Error getting status for txId: {}", txId, e);
            return Response.status(500)
                .entity("{\"error\":\"" + e.getMessage() + "\"}")
                .build();
        }
    }

    /**
     * Get header details by LOG_ID
     */
    @GET
    @Path("/header/{logId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHeader(@PathParam("logId") long logId) {
        try {
            Map<String, Object> header = importService.getHeaderById(logId);
            if (header == null) {
                return Response.status(404)
                    .entity("{\"error\":\"Header not found\"}")
                    .build();
            }
            return Response.ok().entity(mapToJson(header)).build();
        } catch (Exception e) {
            logger.error("Error getting header for logId: {}", logId, e);
            return Response.status(500)
                .entity("{\"error\":\"" + e.getMessage() + "\"}")
                .build();
        }
    }

    /**
     * Get processing summary for a header
     */
    @GET
    @Path("/summary/{logId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSummary(@PathParam("logId") long logId) {
        try {
            List<Map<String, Object>> summary = importService.getProcessingSummary(logId);
            return Response.ok().entity(listToJson(summary)).build();
        } catch (Exception e) {
            logger.error("Error getting summary for logId: {}", logId, e);
            return Response.status(500)
                .entity("{\"error\":\"" + e.getMessage() + "\"}")
                .build();
        }
    }

    /**
     * Health check endpoint
     */
    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Response healthCheck() {
        return Response.ok()
            .entity("{\"status\":\"UP\",\"service\":\"SmartMeters Connector\",\"version\":\"3.0\"}")
            .build();
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private String readRequestBody(InputStream bodyStream) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(bodyStream, StandardCharsets.UTF_8.name()))) {

           String line;

           while ((line = reader.readLine()) != null) {
               sb.append(line).append('\n');
           }
        }
        return sb.toString();
    }

    private String detectSourceSystem(String xmlBody) {
        if (xmlBody.contains("ITRON") || xmlBody.contains("itron")) {
            return MdmImportService.SOURCE_SYSTEM_ITRON;
        }
        return MdmImportService.SOURCE_SYSTEM_ZFA;
    }

    private Response buildSuccessResponse(String txId, long hdLogId) {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<Response>\n" +
            "  <Status>SUCCESS</Status>\n" +
            "  <TransactionId>" + txId + "</TransactionId>\n" +
            "  <LogId>" + hdLogId + "</LogId>\n" +
            "  <Message>Load profile data received and stored successfully</Message>\n" +
            "</Response>";
        return Response.ok(xml).build();
    }

    private Response buildErrorResponse(String txId, String message, int status) {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<Response>\n" +
            "  <Status>ERROR</Status>\n" +
            "  <TransactionId>" + txId + "</TransactionId>\n" +
            "  <Message>" + escapeXml(message) + "</Message>\n" +
            "</Response>";
        return Response.status(status).entity(xml).build();
    }

    private String escapeXml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
    }

    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            Object value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number) {
                sb.append(value);
            } else {
                sb.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private String listToJson(List<Map<String, Object>> list) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Map<String, Object> map : list) {
            if (!first) sb.append(",");
            sb.append(mapToJson(map));
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }
}
