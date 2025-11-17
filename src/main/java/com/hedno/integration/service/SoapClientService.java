package com.hedno.integration.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * SOAP Client Service for sending load profile data to SAP.
 * Implements the Exactly Once (EO) pattern by including UUID in the URL.
 * 
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class SoapClientService {
    
    private static final Logger logger = LoggerFactory.getLogger(SoapClientService.class);
    
    // Configuration constants
    private static final String DEFAULT_CONTENT_TYPE = "text/xml; charset=UTF-8";
    private static final String SOAP_ACTION_HEADER = "SOAPAction";
    private static final int DEFAULT_CONNECT_TIMEOUT = 30000; // 30 seconds
    private static final int DEFAULT_READ_TIMEOUT = 60000; // 60 seconds
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 5000; // 5 seconds
    
    // SAP endpoint configuration
    private final String endpointHost;
    private final int endpointPort;
    private final String endpointPath;
    private final String username;
    private final String password;
    private final boolean useHttps;
    private final int connectTimeout;
    private final int readTimeout;
    
    /**
     * Response class to encapsulate HTTP response details
     */
    public static class SoapResponse {
        private final int statusCode;
        private final String statusMessage;
        private final String responseBody;
        private final boolean success;
        
        public SoapResponse(int statusCode, String statusMessage, String responseBody) {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.responseBody = responseBody;
            this.success = (statusCode == 200);
        }
        
        public int getStatusCode() { return statusCode; }
        public String getStatusMessage() { return statusMessage; }
        public String getResponseBody() { return responseBody; }
        public boolean isSuccess() { return success; }
        
        @Override
        public String toString() {
            return String.format("SoapResponse[status=%d, message=%s, success=%s]", 
                statusCode, statusMessage, success);
        }
    }
    
    /**
     * Constructor with configuration parameters
     */
    public SoapClientService(String host, int port, String path, 
                            String username, String password, boolean useHttps) {
        this.endpointHost = host;
        this.endpointPort = port;
        this.endpointPath = path;
        this.username = username;
        this.password = password;
        this.useHttps = useHttps;
        this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        this.readTimeout = DEFAULT_READ_TIMEOUT;
        
        // Initialize SSL if using HTTPS
        if (useHttps) {
            initializeSSL();
        }
        
        logger.info("SoapClientService initialized for {}://{}:{}{}", 
            useHttps ? "https" : "http", host, port, path);
    }
    
    /**
     * Send SOAP message to SAP with Exactly Once guarantee
     * @param soapXml The SOAP XML message
     * @param messageUuid The unique message ID for Exactly Once processing
     * @return SoapResponse containing the result
     */
    public SoapResponse sendSoapMessage(String soapXml, String messageUuid) {
        return sendSoapMessage(soapXml, messageUuid, 0);
    }
    
    /**
     * Internal method to send SOAP message with retry logic
     */
    private SoapResponse sendSoapMessage(String soapXml, String messageUuid, int attemptNumber) {
        if (attemptNumber >= MAX_RETRY_ATTEMPTS) {
            String error = String.format("Max retry attempts (%d) reached for UUID: %s", 
                MAX_RETRY_ATTEMPTS, messageUuid);
            logger.error(error);
            return new SoapResponse(500, "Max retries exceeded", error);
        }
        
        String urlWithMessageId = buildUrlWithMessageId(messageUuid);
        logger.info("Sending SOAP message to: {} (attempt {}/{})", 
            urlWithMessageId, attemptNumber + 1, MAX_RETRY_ATTEMPTS);
        
        HttpURLConnection connection = null;
        
        try {
            // Create connection
            URL url = new URL(urlWithMessageId);
            connection = (HttpURLConnection) url.openConnection();
            
            // Configure connection
            configureConnection(connection, soapXml.length());
            
            // Write SOAP request
            writeSoapRequest(connection, soapXml);
            
            // Read response
            SoapResponse response = readSoapResponse(connection);
            
            // Log response
            logger.info("Received response for UUID {}: {}", messageUuid, response);
            
            // Handle response based on status code
            if (response.isSuccess()) {
                // HTTP 200 - Success
                logger.info("Successfully sent load profile with UUID: {}", messageUuid);
                return response;
                
            } else if (response.getStatusCode() >= 400 && response.getStatusCode() < 500) {
                // 4xx errors - Client error, should retry
                logger.warn("Client error for UUID {}: {} - Retrying...", 
                    messageUuid, response.getStatusCode());
                
                if (attemptNumber < MAX_RETRY_ATTEMPTS - 1) {
                    Thread.sleep(RETRY_DELAY_MS);
                    return sendSoapMessage(soapXml, messageUuid, attemptNumber + 1);
                }
                
            } else if (response.getStatusCode() >= 500) {
                // 5xx errors - Server error, should retry
                logger.warn("Server error for UUID {}: {} - Retrying...", 
                    messageUuid, response.getStatusCode());
                
                if (attemptNumber < MAX_RETRY_ATTEMPTS - 1) {
                    Thread.sleep(RETRY_DELAY_MS * (attemptNumber + 1)); // Progressive delay
                    return sendSoapMessage(soapXml, messageUuid, attemptNumber + 1);
                }
            }
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error sending SOAP message for UUID: {}", messageUuid, e);
            
            // Retry on exception
            if (attemptNumber < MAX_RETRY_ATTEMPTS - 1) {
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                return sendSoapMessage(soapXml, messageUuid, attemptNumber + 1);
            }
            
            return new SoapResponse(500, "Internal Error", e.getMessage());
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Build the complete URL with Message ID for Exactly Once processing
     */
    private String buildUrlWithMessageId(String messageUuid) {
        StringBuilder url = new StringBuilder();
        
        url.append(useHttps ? "https://" : "http://");
        url.append(endpointHost);
        url.append(":");
        url.append(endpointPort);
        url.append(endpointPath);
        
        // Add query parameters for Exactly Once processing
        url.append("?senderParty=");
        url.append("&senderService=T_MERES");
        url.append("&interface=UtilitiesTimeSeriesERPItemBulkNotification_In");
        url.append("&receiverParty=");
        url.append("&receiverService=EHE000130");
        url.append("&interfaceNamespace=http://sap.com/xi/SAPGlobal20/Global");
        url.append("&MessageId=").append(messageUuid);
        
        return url.toString();
    }
    
    /**
     * Configure the HTTP connection
     */
    private void configureConnection(HttpURLConnection connection, int contentLength) 
            throws IOException {
        
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setUseCaches(false);
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        
        // Set headers
        connection.setRequestProperty("Content-Type", DEFAULT_CONTENT_TYPE);
        connection.setRequestProperty("Content-Length", String.valueOf(contentLength));
        connection.setRequestProperty(SOAP_ACTION_HEADER, "");
        
        // Set authentication if provided
        if (username != null && password != null) {
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes("UTF-8"));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
        }
        
        // Additional headers for SAP
        connection.setRequestProperty("Accept", "text/xml");
        connection.setRequestProperty("Cache-Control", "no-cache");
        connection.setRequestProperty("Pragma", "no-cache");
        connection.setRequestProperty("User-Agent", "HEDNO-LoadProfile-Client/1.0");
    }
    
    /**
     * Write SOAP request to the connection
     */
    private void writeSoapRequest(HttpURLConnection connection, String soapXml) 
            throws IOException {
        
        try (OutputStream os = connection.getOutputStream();
             OutputStreamWriter writer = new OutputStreamWriter(os, "UTF-8")) {
            
            writer.write(soapXml);
            writer.flush();
            
            logger.debug("SOAP request sent, size: {} bytes", soapXml.length());
        }
    }
    
    /**
     * Read SOAP response from the connection
     */
    private SoapResponse readSoapResponse(HttpURLConnection connection) 
            throws IOException {
        
        int statusCode = connection.getResponseCode();
        String statusMessage = connection.getResponseMessage();
        
        // Read response body (from input or error stream based on status)
        String responseBody = "";
        InputStream inputStream = null;
        
        try {
            if (statusCode >= 200 && statusCode < 300) {
                inputStream = connection.getInputStream();
            } else {
                inputStream = connection.getErrorStream();
            }
            
            if (inputStream != null) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(inputStream, "UTF-8"))) {
                    
                    StringBuilder response = new StringBuilder();
                    String line;
                    
                    while ((line = reader.readLine()) != null) {
                        response.append(line).append("\n");
                    }
                    
                    responseBody = response.toString();
                }
            }
            
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
        
        logger.debug("Response received - Status: {}, Body length: {} bytes", 
            statusCode, responseBody.length());
        
        return new SoapResponse(statusCode, statusMessage, responseBody);
    }
    
    /**
     * Initialize SSL for HTTPS connections (with certificate validation)
     */
    private void initializeSSL() {
        try {
            // For production, use proper certificate validation
            // This is a placeholder for development
            SSLContext sslContext = SSLContext.getInstance("TLS");
            
            // In production, load the proper truststore with SAP certificates
            // For now, using default trust manager
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((java.security.KeyStore) null);
            
            sslContext.init(null, tmf.getTrustManagers(), null);
            
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            
            // Hostname verification (strict in production)
            HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    // In production, verify hostname matches certificate
                    return hostname.equals(endpointHost);
                }
            });
            
            logger.info("SSL initialized for HTTPS connections");
            
        } catch (Exception e) {
            logger.error("Failed to initialize SSL", e);
            throw new RuntimeException("SSL initialization failed", e);
        }
    }
    
    /**
     * Test connectivity to the SAP endpoint
     * @return true if endpoint is reachable
     */
    public boolean testConnection() {
        try {
            URL url = new URL(buildUrlWithMessageId("TEST-" + System.currentTimeMillis()));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            
            int responseCode = connection.getResponseCode();
            connection.disconnect();
            
            logger.info("Connection test result: {}", responseCode);
            return responseCode > 0;
            
        } catch (Exception e) {
            logger.error("Connection test failed", e);
            return false;
        }
    }
}
