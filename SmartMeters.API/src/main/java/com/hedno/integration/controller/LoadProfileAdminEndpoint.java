package com.hedno.integration.controller;

import com.hedno.integration.dao.LoadProfileInboundDAO;
import com.hedno.integration.entity.LoadProfileInbound;
import com.hedno.integration.entity.LoadProfileInbound.ProcessingStatus;
import com.hedno.integration.processor.LoadProfileProcessorAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.EJB;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Administrative REST endpoint for monitoring and manually retrying
 * outbound Load Profile SOAP messages.
 *
 * Base URL (with RestApplication @ApplicationPath("/api")):
 *   /api/load-profile/admin/...
 *
 * This endpoint is intended to be called by the Order-Management portal.
 */
@Path("/load-profile/admin")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LoadProfileAdminEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileAdminEndpoint.class);

    // Use DAO directly (same pattern as in other parts of the app)
    private final LoadProfileInboundDAO inboundDAO = new LoadProfileInboundDAO();

    // EJB that sends SOAP messages and implements manualRetry(...)
    @EJB
    private LoadProfileProcessorAsync asyncProcessor;

    private static final DateTimeFormatter ISO_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * Lightweight DTO exposed to the portal.
     * RAW_PAYLOAD δεν εκτίθεται για να μην είναι τεράστια τα responses.
     */
    public static class LoadProfileMessageDTO {
        public String messageUuid;
        public String status;
        public String originalMessageId;

        public String receivedTimestamp;
        public String processingStartTime;
        public String processingEndTime;
        public String lastManualRetryTime;

        public int lastHttpStatusCode;
        public String lastResponseMessage;
        public String lastErrorMessage;

        public int attemptCount;
        public int manualRetryCount;

        public long processingDurationMs;
        public long ageHours;
    }

    /**
     * Λίστα “jobs” (LoadProfileInbound messages) για το portal.
     *
     * Examples:
     *   GET /api/load-profile/admin/messages
     *   GET /api/load-profile/admin/messages?status=FAILED
     *   GET /api/load-profile/admin/messages?status=COMPLETED&limit=100
     */
    @GET
    @Path("/messages")
    public Response listMessages(@QueryParam("status") String status,
                                 @QueryParam("limit") @DefaultValue("200") int limit) {
        try {
            List<LoadProfileInbound> entities = new ArrayList<>();

            if (status == null || status.trim().isEmpty()
                    || "ALL".equalsIgnoreCase(status)) {
                // Όλα τα statuses (PENDING, PROCESSING, COMPLETED, FAILED)
                for (ProcessingStatus st : ProcessingStatus.values()) {
                    entities.addAll(inboundDAO.findByStatus(st));
                }
            } else {
                ProcessingStatus st = ProcessingStatus.fromValue(status.toUpperCase());
                entities = inboundDAO.findByStatus(st);
            }

            // Απλό limit in-memory (DB ήδη κάνει ORDER BY RECEIVED_TIMESTAMP)
            if (entities.size() > limit) {
                entities = entities.subList(0, limit);
            }

            List<LoadProfileMessageDTO> dtos = new ArrayList<>();
            for (LoadProfileInbound entity : entities) {
                dtos.add(toDto(entity));
            }

            return Response.ok(dtos).build();
        } catch (IllegalArgumentException ex) {
            log.warn("Invalid status parameter: {}", status, ex);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid status value: " + status)
                    .build();
        } catch (Exception e) {
            log.error("Error while listing load profile messages", e);
            return Response.serverError()
                    .entity("Internal error while listing messages")
                    .build();
        }
    }

    /**
     * Λεπτομέρειες για ένα συγκεκριμένο job/message.
     *
     * Example:
     *   GET /api/load-profile/admin/messages/{uuid}
     */
    @GET
    @Path("/messages/{uuid}")
    public Response getMessage(@PathParam("uuid") String messageUuid) {
        try {
            LoadProfileInbound entity = inboundDAO.findByUuid(messageUuid);
            if (entity == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Message not found: " + messageUuid)
                        .build();
            }
            return Response.ok(toDto(entity)).build();
        } catch (Exception e) {
            log.error("Error while fetching message {}", messageUuid, e);
            return Response.serverError()
                    .entity("Internal error while fetching message")
                    .build();
        }
    }

    /**
     * Manual retry για FAILED job.
     *
     * Requirements που καλύπτει:
     *   - Δεν υπάρχει automatic retry (μόνο manual).
     *   - Η manual επανάληψη χρησιμοποιεί ΑΚΡΙΒΩΣ το ίδιο XML payload
     *     και το ίδιο MessageId (UUID) όπως η πρώτη προσπάθεια.
     *
     * Implementation:
     *   - asyncProcessor.manualRetry(...) κάνει reset σε PENDING,
     *     κρατάει messageUuid + RAW_PAYLOAD.
     *   - Ο LoadProfileProcessorAsync θα το ξαναστείλει προς SAP.
     *
     * Example:
     *   POST /api/load-profile/admin/messages/{uuid}/retry
     */
    @POST
    @Path("/messages/{uuid}/retry")
    public Response retryMessage(@PathParam("uuid") String messageUuid) {
        try {
            LoadProfileInbound entity = inboundDAO.findByUuid(messageUuid);
            if (entity == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity("Message not found: " + messageUuid)
                        .build();
            }

            if (!entity.getStatus().allowsManualRetry()) {
                return Response.status(Response.Status.CONFLICT)
                        .entity("Message is not in FAILED status. Current status: "
                                + entity.getStatus().getValue())
                        .build();
            }

            boolean ok = asyncProcessor.manualRetry(messageUuid);
            if (!ok) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Unable to reset message for retry: " + messageUuid)
                        .build();
            }

            log.info("Manual retry submitted for message {}", messageUuid);
            return Response.ok()
                    .entity("Manual retry submitted for message: " + messageUuid)
                    .build();
        } catch (Exception e) {
            log.error("Error while retrying message {}", messageUuid, e);
            return Response.serverError()
                    .entity("Internal error while retrying message")
                    .build();
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private LoadProfileMessageDTO toDto(LoadProfileInbound entity) {
        LoadProfileMessageDTO dto = new LoadProfileMessageDTO();
        dto.messageUuid = entity.getMessageUuid();
        dto.status = entity.getStatus() != null ? entity.getStatus().getValue() : null;
        dto.originalMessageId = entity.getOriginalMessageId();

        dto.receivedTimestamp = format(entity.getReceivedTimestamp());
        dto.processingStartTime = format(entity.getProcessingStartTime());
        dto.processingEndTime = format(entity.getProcessingEndTime());
        dto.lastManualRetryTime = format(entity.getLastManualRetryTime());

        dto.lastHttpStatusCode = entity.getLastHttpStatusCode();
        dto.lastResponseMessage = entity.getLastResponseMessage();
        dto.lastErrorMessage = entity.getLastErrorMessage();

        dto.attemptCount = entity.getAttemptCount();
        dto.manualRetryCount = entity.getManualRetryCount();

        dto.processingDurationMs = entity.getProcessingDurationMs();
        dto.ageHours = entity.getAgeInHours();

        return dto;
    }

    private String format(Timestamp ts) {
        if (ts == null) {
            return null;
        }
        return ts.toLocalDateTime().format(ISO_FORMATTER);
    }
}
