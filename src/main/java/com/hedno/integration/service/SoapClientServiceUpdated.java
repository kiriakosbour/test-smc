package com.hedno.integration.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 * Updated SOAP Client Service with improved logging and status handling
 * 
 * Changes:
 * - Logs only headers and profile count, not full XML
 * - Properly returns 2xx, 4xx, 5xx status codes
 * - Supports configurable endpoint
 * 
 * @author HEDNO Integration Team
 * @version 2.0
 */
public class SoapClientServiceUpdated {
    
    private static final Logger logger = LoggerFactory.getLogger(SoapClientServiceUpdated.class);
    
    // Configuration constants
    private static final String SOAP_1_1_CONTENT_TYPE = "text/xml; charset=UTF-8";
    private static final String SOAP_ACTION_HEADER = "SOAPAction";
    private static final String USER_AGENT = "HEDNO-LoadProfile-Client/2.0";
    private static final int DEFAULT_CONNECT_TIMEOUT = 30000; // 30 seconds
    private static final int DEFAULT_READ_TIMEOUT = 60000; // 60 seconds
    
    // Endpoint configuration
    private final String endpointUrl;
    private final String username;
    private final String password;
    private final boolean useHttps;
    private final int connectTimeout;
    private final int readTimeout;
    
    // SSL Context
    private SSLContext sslContext;
    private HostnameVerifier hostnameVerifier;
    
    /**
     * Response class to encapsulate HTTP response details
     */
    public static class SoapResponse {
        private final int statusCode;
        private final String statusMessage;
        private final String messageId;
        private final Map<String, String> responseHeaders;
        private final boolean success;
        
        public SoapResponse(int statusCode, String statusMessage, String messageId, Map<String, String> headers) {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.messageId = messageId;
            this.responseHeaders = headers;
            // 2xx responses are considered success
            this.success = (statusCode >= 200 && statusCode < 300);
        }
        
        public int getStatusCode() { return statusCode; }
        public String getStatusMessage() { return statusMessage; }
        public String getMessageId() { return messageId; }
        public Map<String, String> getResponseHeaders() { return responseHeaders; }
        public boolean isSuccess() { return success; }
        
        @Override
        public String toString() {
            return String.format("SoapResponse[messageId=%s, status=%d, message=%s, success=%s]", 
                messageId, statusCode, statusMessage, success);
        }
    }
    
    /**
     * Constructor with full URL endpoint
     */
    public SoapClientServiceUpdated(String endpointUrl, String username, String password) {
        this.endpointUrl = endpointUrl;
        this.username = username;
        this.password = password;
        this.useHttps = endpointUrl.startsWith("https");
        this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        this.readTimeout = DEFAULT_READ_TIMEOUT;
        
        // Initialize SSL if using HTTPS
        if (useHttps) {
            initializeSSL();
        }
        
        logger.info("SoapClientServiceUpdated initialized for endpoint: {}", endpointUrl);
    }
    
    /**
     * Send SOAP message with proper status code handling
     * @param soapXml The SOAP XML message
     * @param messageUuid The unique message ID
     * @param profileCount The number of profiles in the message
     * @return SoapResponse with appropriate status code
     */
    public SoapResponse sendSoapMessage(String soapXml, String messageUuid, int profileCount) {
        long startTime = System.currentTimeMillis();
        HttpURLConnection connection = null;
        
        // Log the request headers and profile count (NOT the full XML)
        logger.info("=== SOAP Request ===");
        logger.info("Message UUID: {}", messageUuid);
        logger.info("Profile Count: {}", profileCount);
        logger.info("Endpoint: {}", endpointUrl);
        logger.info("Content-Length: {} bytes", soapXml.getBytes(StandardCharsets.UTF_8).length);
        
        try {
            // Create connection
            URL url = new URL(endpointUrl);
            connection = (HttpURLConnection) url.openConnection();
            
            // Apply SSL settings if HTTPS
            if (useHttps && connection instanceof HttpsURLConnection) {
                if (sslContext != null) {
                    ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
                }
                if (hostnameVerifier != null) {
                    ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
                }
            }
            
            // Configure connection
            configureConnection(connection, soapXml.getBytes(StandardCharsets.UTF_8).length);
            
            // Log request headers
            Map<String, List<String>> requestHeaders = connection.getRequestProperties();
            logger.debug("Request Headers:");
            for (Map.Entry<String, List<String>> entry : requestHeaders.entrySet()) {
                logger.debug("  {}: {}", entry.getKey(), entry.getValue());
            }
            
            // Send SOAP request
            writeSoapRequest(connection, soapXml);
            
            // Get response
            int statusCode = connection.getResponseCode();
            String statusMessage = connection.getResponseMessage();
            long responseTime = System.currentTimeMillis() - startTime;
            
            // Capture response headers
            Map<String, String> responseHeaders = new HashMap<>();
            Map<String, List<String>> headerFields = connection.getHeaderFields();
            if (headerFields != null) {
                for (Map.Entry<String, List<String>> entry : headerFields.entrySet()) {
                    if (entry.getKey() != null && entry.getValue() != null && !entry.getValue().isEmpty()) {
                        responseHeaders.put(entry.getKey(), entry.getValue().get(0));
                    }
                }
            }
            
            // Log response headers and status (NOT the response body)
            logger.info("=== SOAP Response ===");
            logger.info("Message UUID: {}", messageUuid);
            logger.info("HTTP Status: {} {}", statusCode, statusMessage);
            logger.info("Response Time: {} ms", responseTime);
            logger.debug("Response Headers:");
            for (Map.Entry<String, String> entry : responseHeaders.entrySet()) {
                logger.debug("  {}: {}", entry.getKey(), entry.getValue());
            }
            
            // Create response object
            SoapResponse response = new SoapResponse(statusCode, statusMessage, messageUuid, responseHeaders);
            
            // Log outcome based on status code
            if (statusCode >= 200 && statusCode < 300) {
                logger.info("SUCCESS: Message {} sent successfully with {} profiles (HTTP {})", 
                    messageUuid, profileCount, statusCode);
            } else if (statusCode >= 400 && statusCode < 500) {
                logger.warn("CLIENT ERROR: Message {} failed with client error (HTTP {})", 
                    messageUuid, statusCode);
            } else if (statusCode >= 500) {
                logger.error("SERVER ERROR: Message {} failed with server error (HTTP {})", 
                    messageUuid, statusCode);
            } else {
                logger.warn("UNEXPECTED: Message {} received unexpected status (HTTP {})", 
                    messageUuid, statusCode);
            }
            
            return response;
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            logger.error("EXCEPTION: Error sending SOAP message {} after {} ms: {}", 
                messageUuid, responseTime, e.getMessage());
            
            // Return 500 status for exceptions
            return new SoapResponse(500, "Internal Error: " + e.getMessage(), 
                messageUuid, new HashMap<>());
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Configure the HTTP connection for SOAP 1.1
     */
    private void configureConnection(HttpURLConnection connection, int contentLength) 
            throws IOException {
        
        // HTTP method
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setUseCaches(false);
        
        // Timeouts
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        
        // SOAP 1.1 specific headers
        connection.setRequestProperty("Content-Type", SOAP_1_1_CONTENT_TYPE);
        connection.setRequestProperty("Content-Length", String.valueOf(contentLength));
        connection.setRequestProperty(SOAP_ACTION_HEADER, ""); // Empty for document/literal
        
        // Authentication
        if (username != null && password != null && !username.isEmpty()) {
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
        }
        
        // Additional headers
        connection.setRequestProperty("Accept", "text/xml, application/xml");
        connection.setRequestProperty("Cache-Control", "no-cache");
        connection.setRequestProperty("Pragma", "no-cache");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setRequestProperty("Connection", "close");
    }
    
    /**
     * Write SOAP request to the connection
     */
    private void writeSoapRequest(HttpURLConnection connection, String soapXml) 
            throws IOException {
        
        try (OutputStream os = connection.getOutputStream();
             OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            
            writer.write(soapXml);
            writer.flush();
            
            logger.debug("SOAP request body sent, size: {} bytes", soapXml.length());
        }
    }
    
    /**
     * Initialize SSL for HTTPS connections
     */
    private void initializeSSL() {
        try {
            // Create a trust manager that accepts all certificates (for testing)
            // In production, use proper certificate validation
            TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }
            };
            
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            
            // Create hostname verifier
            hostnameVerifier = new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    // In production, implement proper hostname verification
                    logger.debug("Verifying hostname: {}", hostname);
                    return true; // Accept all for testing
                }
            };
            
            logger.info("SSL initialized for HTTPS connections");
            
        } catch (Exception e) {
            logger.error("Failed to initialize SSL", e);
            throw new RuntimeException("SSL initialization failed", e);
        }
    }
    
    /**
     * Test connectivity to the endpoint
     * @return true if endpoint is reachable
     */
    public boolean testConnection() {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(endpointUrl);
            connection = (HttpURLConnection) url.openConnection();
            
            // Apply SSL settings if HTTPS
            if (useHttps && connection instanceof HttpsURLConnection) {
                if (sslContext != null) {
                    ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
                }
                if (hostnameVerifier != null) {
                    ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
                }
            }
            
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            
            if (username != null && password != null && !username.isEmpty()) {
                String auth = username + ":" + password;
                String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            }
            
            int responseCode = connection.getResponseCode();
            
            logger.info("Connection test result: HTTP {}", responseCode);
            return responseCode > 0;
            
        } catch (Exception e) {
            logger.error("Connection test failed", e);
            return false;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Extract profile count from XML (helper method)
     */
    public static int extractProfileCount(String xml) {
        try {
            // Count occurrences of profile/item elements
            int count = 0;
            int index = 0;
            String searchTag = "<Profile>"; // Adjust based on your XML format
            
            while ((index = xml.indexOf(searchTag, index)) != -1) {
                count++;
                index += searchTag.length();
            }
            
            if (count == 0) {
                // Try alternative tags
                searchTag = "<Item>";
                index = 0;
                while ((index = xml.indexOf(searchTag, index)) != -1) {
                    count++;
                    index += searchTag.length();
                }
            }
            
            return count;
        } catch (Exception e) {
            logger.error("Failed to extract profile count from XML", e);
            return -1;
        }
    }
}
