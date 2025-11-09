package com.hedno.integration.controller;

import com.hedno.integration.dao.OrderPackageDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
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

@Path("/load-profile")
@Consumes({MediaType.APPLICATION_XML, MediaType.TEXT_XML})
@Produces(MediaType.TEXT_PLAIN)
public class LoadProfileReceiverEndpoint {

    private static final Logger log = LoggerFactory.getLogger(LoadProfileReceiverEndpoint.class);

    @Inject
    private OrderPackageDAO orderPackageDAO;

    /**
     * Simple health-check.
     * GET /load-profile-push-api/api/load-profile/ping
     */
    @GET
    @Path("/ping")
    public Response ping() {
        log.info("Ping called");
        return Response.ok("pong").build();
    }

    /**
     * Main endpoint that receives the ProfilBloc XML.
     * POST /load-profile-push-api/api/load-profile/data
     */
    @POST
    @Path("/data")
    public Response receiveProfileData(String xmlBody) {
        if (xmlBody == null || xmlBody.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST).entity("XML body is empty.").build();
        }

        try {
            log.info("Received PROFIL_BLOC XML, size={} bytes", xmlBody.length());

            ProfilBlocDto dto = parseProfilBlocXml(xmlBody);

            if (!isPushRelevant(dto.getChannelId())) {
                log.info("Channel {} is NOT push relevant → ignoring", dto.getChannelId());
                return Response.accepted("Channel not push relevant, ignored.").build();
            }

            orderPackageDAO.createOrderItem(
                    dto.getChannelId(),
                    dto.getProfilBlocId(),
                    dto.getDataType(),
                    dto.getObisCode(),
                    dto.getPodId(),
                    xmlBody
            );

            log.info("Order item created for PROFIL_BLOC {} / channel {}",
                    dto.getProfilBlocId(), dto.getChannelId());

            return Response.ok("Order item created.").build();

        } catch (Exception e) {
            log.error("Error while processing PROFIL_BLOC XML", e);
            return Response.serverError().entity("Error: " + e.getMessage()).build();
        }
    }

    // ---------- Helpers ----------

    private ProfilBlocDto parseProfilBlocXml(String xmlBody) throws Exception {
        // REFACTOR: Use try-with-resources for the InputStream
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

    private String getTagValue(Document doc, String tag) {
        try {
            return doc.getElementsByTagName(tag).item(0).getTextContent();
        } catch (Exception e) {
            log.warn("Could not find or parse tag: {}", tag);
            return null;
        }
    }

    private boolean isPushRelevant(String channelId) {
        // This check is now implemented in the DAO
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