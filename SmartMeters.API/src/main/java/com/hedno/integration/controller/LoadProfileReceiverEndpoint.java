package com.hedno.integration.controller;

import com.hedno.integration.dao.IntervalDataDAO;
import com.hedno.integration.dao.OrderPackageDAO;
import com.hedno.integration.processor.IntervalData;
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
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * REFACTORED Endpoint.
 * This endpoint is now SYNCHRONOUS and TRANSACTIONAL.
 * It receives the XML, parses it, and saves all 96 intervals
 * before returning a 2xx or 5xx response.
 * This ensures the external caller can retry on failure.
 */
@Path("/load-profile")
@Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML})
@Produces(MediaType.TEXT_PLAIN)
public class LoadProfileReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileReceiverEndpoint.class);
    private OrderPackageDAO orderPackageDAO;
    private IntervalDataDAO intervalDataDAO;
    private IntervalParsingService parsingService;

    /**
     * Initialize all DAOs and Services
     */
    @PostConstruct
    public void init() {
        this.orderPackageDAO = new OrderPackageDAO();
        this.intervalDataDAO = new IntervalDataDAO();
        this.parsingService = new IntervalParsingService();
        log.info("LoadProfileReceiverEndpoint (SYNCHRONOUS) initialized.");
    }

    /**
     * Simple health-check.
     */
    @GET
    @Path("/ping")
    public Response ping() {
        log.info("Ping called");
        return Response.ok("pong").build();
    }

    /**
     * Main endpoint that receives, parses, and saves ProfilBloc XML.
     * This is now a single transaction.
     */
    @POST
    @Path("/data")
    public Response receiveProfileData(String xmlBody) {
        if (xmlBody == null || xmlBody.isEmpty()) {
            // 4xx Response: Bad data
            return Response.status(Response.Status.BAD_REQUEST).entity("XML body is empty.").build();
        }

        try {
            log.info("Received PROFIL_BLOC XML, size={} bytes. Processing SYNCHRONOUSLY.", xmlBody.length());

            // 1. Parse the XML to get metadata
            ProfilBlocDto dto = parseProfilBlocXml(xmlBody);

            // 2. Check for relevance
            if (!isPushRelevant(dto.getChannelId())) {
                log.info("Channel {} is NOT push relevant -> ignoring", dto.getChannelId());
                // 2xx Response: Accepted but not processed
                return Response.accepted("Channel not push relevant, ignored.").build();
            }

            // 3. Save the "parent" record to SMC_ORDER_ITEMS
            // This creates the item with "PENDING" status and gets its new ITEM_ID.
            long itemId = orderPackageDAO.createOrderItem(
                    dto.getChannelId(),
                    dto.getProfilBlocId(),
                    dto.getDataType(),
                    dto.getObisCode(),
                    dto.getPodId(),
                    xmlBody
            );

            // 4. Parse the 96 intervals from the raw XML
            List<IntervalData> intervals = parsingService.parseIntervalsFromXml(xmlBody, dto.getProfilBlocId());

            if (intervals.isEmpty()) {
                log.error("Failed to parse any intervals from ProfilBlocId: {}", dto.getProfilBlocId());
                // 5xx Response: Parsing failed, tell caller to retry
                return Response.serverError().entity("Could not parse intervals from XML.").build();
            }

            // 5. Batch-insert the 96 intervals into the database
            boolean success = intervalDataDAO.batchInsertIntervals(itemId, intervals);
            
            if (!success) {
                // 5xx Response: Database save failed, tell caller to retry
                log.error("Failed to save intervals to database for ProfilBlocId: {}", dto.getProfilBlocId());
                return Response.serverError().entity("Failed to save intervals to DB.").build();
            }

            // 6. Mark the parent item as 'PROCESSED'
            orderPackageDAO.updateItemStatus(itemId, OrderItem.ItemStatus.PROCESSED);

            log.info("Successfully processed and saved {} intervals for ProfilBlocId: {}",
                    intervals.size(), dto.getProfilBlocId());

            // 2xx Response: Success!
            return Response.ok("Data saved successfully. Intervals: " + intervals.size()).build();

        } catch (Exception e) {
            log.error("Generic error while processing PROFIL_BLOC XML: {}", e.getMessage(), e);
            // 5xx Response: Unknown error, tell caller to retry
            return Response.serverError().entity("Error: " + e.getMessage()).build();
        }
    }
    
    // ---------- Helpers ----------

    private ProfilBlocDto parseProfilBlocXml(String xmlBody) throws Exception {
        // Use try-with-resources for the InputStream
        try (InputStream stream = new ByteArrayInputStream(xmlBody.getBytes(StandardCharsets.UTF_8))) {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(stream);

            ProfilBlocDto dto = new ProfilBlocDto();
            dto.setProfilBlocId(getTagValue(doc, "ProfilBlocId"));
            dto.setChannelId(getTagValue(doc, "ChannelId"));
            dto.setDataType(getTagValue(doc, "DataType"));
            dto.setObisCode(getTagValue(doc, "ObisCode"));
            dto.setPodId(getTagValue(doc, "PodId"));
            return dto;
        }
    }

    private String getTagValue(Document doc, String tag) throws Exception {
        try {
            return doc.getElementsByTagName(tag).item(0).getTextContent();
        } catch (Exception e) {
            log.warn("Could not find or parse tag: {}", tag);
            throw new Exception("Missing required XML tag: " + tag);
        }
    }

    private boolean isPushRelevant(String channelId) {
        return orderPackageDAO.isChannelPushRelevant(channelId);
    }
    
    // Inner class for data transfer
    public static class ProfilBlocDto {
        private String profilBlocId;
        private String channelId;
        private String dataType;
        private String obisCode;
        private String podId;

        // Getters and Setters
        public String getProfilBlocId() { return profilBlocId; }
        public void setProfilBlocId(String profilBlocId) { this.profilBlocId = profilBlocId; }
        public String getChannelId() { return channelId; }
        public void setChannelId(String channelId) { this.channelId = channelId; }
        public String getDataType() { return dataType; }
        public void setDataType(String dataType) { this.dataType = dataType; }
        public String getObisCode() { return obisCode; }
        public void setObisCode(String obisCode) { this.obisCode = obisCode; }
        public String getPodId() { return podId; }
        public void setPodId(String podId) { this.podId = podId; }
    }
}