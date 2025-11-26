package com.hedno.integration.controller;

import com.hedno.integration.dao.IntervalDataDAO;
import com.hedno.integration.dao.OrderPackageDAO;
import com.hedno.integration.processor.IntervalData;
import com.hedno.integration.processor.LoadProfileData;
import com.hedno.integration.service.IntervalParsingService;
import com.hedno.integration.entity.OrderItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * REFACTORED Endpoint.
 * Receives SOAP Bulk Notification XML at /api/profiles.
 * Parses all profiles, checks relevance, and saves data transactionally.
 */
@Path("/profiles")
@Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML})
@Produces(MediaType.TEXT_PLAIN)
public class LoadProfileReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileReceiverEndpoint.class);
    
    private OrderPackageDAO orderPackageDAO;
    private IntervalDataDAO intervalDataDAO;
    private IntervalParsingService parsingService;

    @PostConstruct
    public void init() {
        this.orderPackageDAO = new OrderPackageDAO();
        this.intervalDataDAO = new IntervalDataDAO();
        this.parsingService = new IntervalParsingService();
        log.info("LoadProfileReceiverEndpoint initialized.");
    }

    @GET
    @Path("/ping")
    public Response ping() {
        return Response.ok("pong").build();
    }

    /**
     * Receives the Bulk XML, processes all profiles within it transactionally.
     * Mapped to POST /profiles (inherited from class @Path).
     */
    @POST
    public Response receiveProfileData(String xmlBody) {
        if (xmlBody == null || xmlBody.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("XML body is empty.").build();
        }

        try {
            log.info("Received Bulk XML payload, size={} bytes. Processing...", xmlBody.length());

            // 1. Parse all profiles from the XML (Bulk Support)
            List<LoadProfileData> profiles = parsingService.parseAllProfiles(xmlBody);

            if (profiles.isEmpty()) {
                log.warn("No valid profiles found in XML.");
                return Response.status(Response.Status.BAD_REQUEST).entity("No valid profiles found.").build();
            }

            int successCount = 0;
            int ignoredCount = 0;

            // 2. Process each profile individually
            for (LoadProfileData profile : profiles) {
                if (processSingleProfile(profile, xmlBody)) {
                    successCount++;
                } else {
                    ignoredCount++;
                }
            }

            log.info("Bulk Processing Result: {} saved, {} ignored/failed, {} total.", 
                     successCount, ignoredCount, profiles.size());

            return Response.ok("Processed " + successCount + " profiles.").build();

        } catch (Exception e) {
            log.error("Error processing bulk XML", e);
            return Response.serverError().entity("Server Error: " + e.getMessage()).build();
        }
    }

    /**
     * Helper to process a single profile: Check Relevance -> Create Item -> Save Intervals -> Update Status.
     */
    private boolean processSingleProfile(LoadProfileData profile, String fullXmlBody) {
        String channelId = profile.getPodId(); // POD ID is used as Channel ID for relevance
        String profilBlocId = profile.getProfilBlocId(); // UUID

        try {
            // Business Logic: Check Relevance
            if (!orderPackageDAO.isChannelPushRelevant(channelId)) {
                log.debug("Channel {} is not push relevant (ProfilBlocId: {}).", channelId, profilBlocId);
                return false;
            }

            // Business Logic: Create Order Item (Parent Record)
            long itemId = orderPackageDAO.createOrderItem(
                    channelId,
                    profilBlocId,
                    "Energie / Consumption",
                    profile.getObisCode(),
                    profile.getPodId(),
                    fullXmlBody // Store full XML for reference
            );

            // Business Logic: Save Intervals
            List<IntervalData> intervals = profile.getIntervals();
            if (intervals != null && !intervals.isEmpty()) {
                boolean saved = intervalDataDAO.batchInsertIntervals(itemId, intervals);
                
                if (saved) {
                    // Business Logic: Update Status to PROCESSED
                    orderPackageDAO.updateItemStatus(itemId, OrderItem.ItemStatus.PROCESSED);
                    return true;
                } else {
                    log.error("Failed to save intervals for item {}. Transaction rolled back.", itemId);
                    return false;
                }
            } else {
                log.warn("No intervals found for ProfilBlocId: {}", profilBlocId);
                return false;
            }
        } catch (Exception e) {
            log.error("Error processing profile (BlocId: {}): {}", profilBlocId, e.getMessage());
            return false;
        }
    }
}