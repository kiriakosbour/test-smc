package com.hedno.integration.controller;

import com.hedno.integration.service.MdmImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/mdm")
public class LoadProfileReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileReceiverEndpoint.class);
    private MdmImportService mdmImportService;

    @PostConstruct
    public void init() {
        // In a non-Spring environment, instantiate manually
        this.mdmImportService = new MdmImportService();
        log.info("MdmImportService initialized.");
    }

    @GET
    @Path("/ping")
    public Response ping() {
        return Response.ok("MDM Receiver Active").build();
    }

    /**
     * DEBUG ENDPOINT (Requested)
     * Retrieves the status of a specific transaction or the latest log.
     */
    @GET
    @Path("/debug/{transactionId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDebugInfo(@PathParam("transactionId") String transactionId) {
        try {
            String status = mdmImportService.getLogStatus(transactionId);
            return Response.ok("{\"transactionId\": \"" + transactionId + "\", \"status\": \"" + status + "\"}").build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error retrieving debug info: " + e.getMessage()).build();
        }
    }

    /**
     * Main Push Endpoint
     * 1. Logs raw request to SMC_MDM_DEBUG_LOG.
     * 2. Parses XML.
     * 3. Transforms Vertical Data -> Horizontal (Q1..Q100).
     * 4. Saves to SMC_MDM_SCCURVES.
     */
    @POST
    @Path("/push")
    @Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML})
    @Produces(MediaType.TEXT_PLAIN)
    public Response receiveMdmPush(String xmlBody) {
        String transactionId = java.util.UUID.randomUUID().toString();
        log.info("Received MDM Push. TxId: {}, Size: {}", transactionId, (xmlBody != null ? xmlBody.length() : 0));

        if (xmlBody == null || xmlBody.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("Empty Body").build();
        }

        try {
            // Service handles the full transaction, including logging to DEBUG_LOG
            mdmImportService.processIncomingXml(transactionId, xmlBody);
            
            return Response.ok("OK. TransactionId: " + transactionId).build();
        } catch (Exception e) {
            log.error("Fatal processing error TxId: {}", transactionId, e);
            return Response.serverError().entity("Processing Failed: " + e.getMessage()).build();
        }
    }
}