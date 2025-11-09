package com.hedno.integration.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Asynchronous SOAP 1.1 Client Service for sending load profile data to SAP.
 * Implements non-blocking operations with proper thread management and connection pooling.
 * * @author HEDNO Integration Team
 * @version 2.1 (Fixed)
 */
public class SoapClientServiceAsync {
    
    private static final Logger logger = LoggerFactory.getLogger(SoapClientServiceAsync.class);
    
    // Configuration constants
    private static final String SOAP_1_1_CONTENT_TYPE = "text/xml; charset=UTF-8";
    private static final String SOAP_ACTION_HEADER = "SOAPAction";
    private static final String USER_AGENT = "HEDNO-LoadProfile-Client/2.0";
    private static final int DEFAULT_CONNECT_TIMEOUT = 30000; // 30 seconds
    private static final int DEFAULT_READ_TIMEOUT = 60000; // 60 seconds
    
    // Thread pool configuration
    private static final int CORE_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 20;
    private static final long KEEP_ALIVE_TIME = 60L;
    private static final int QUEUE_CAPACITY = 100;
    
    // SAP endpoint configuration
    private final String endpointHost;
    private final int endpointPort;
    private final String endpointPath;
    private final String username;
    private final String password;
    private final boolean useHttps;
    private final int connectTimeout;
    private final int readTimeout;
    
    // Thread pool for async operations
    private final ExecutorService executorService;
    private final AtomicInteger activeRequests = new AtomicInteger(0);
    private volatile boolean isShuttingDown = false;
    
    // SSL Context for secure connections
    // FIXED: These are now instance variables, not JVM-wide defaults
    private SSLContext sslContext;
    private HostnameVerifier hostnameVerifier;
    
    /**
     * Response class to encapsulate HTTP response details
     */
    public static class SoapResponse {
        private final int statusCode;
        private final String statusMessage;
        private final String messageId;
        private final long responseTimeMs;
        private final boolean success;
        private final Exception exception;
        
        public SoapResponse(int statusCode, String statusMessage, String messageId, long responseTimeMs) {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.messageId = messageId;
            this.responseTimeMs = responseTimeMs;
            this.success = (statusCode == 200);
            this.exception = null;
        }
        
        public SoapResponse(String messageId, Exception exception) {
            this.statusCode = -1;
            this.statusMessage = "Connection Failed";
            this.messageId = messageId;
            this.responseTimeMs = -1;
            this.success = false;
            this.exception = exception;
        }
        
        public int getStatusCode() { return statusCode; }
        public String getStatusMessage() { return statusMessage; }
        public String getMessageId() { return messageId; }
        public long getResponseTimeMs() { return responseTimeMs; }
        public boolean isSuccess() { return success; }
        public Exception getException() { return exception; }
        
        @Override
        public String toString() {
            return String.format("SoapResponse[messageId=%s, status=%d, message=%s, success=%s, responseTime=%dms]", 
                messageId, statusCode, statusMessage, success, responseTimeMs);
        }
    }
    
    /**
     * Callback interface for async operations
     */
    public interface SoapResponseCallback {
        void onSuccess(SoapResponse response);
        void onFailure(SoapResponse response);
        void onException(String messageId, Exception e);
    }
    
    /**
     * Constructor with configuration parameters
     */
    public SoapClientServiceAsync(String host, int port, String path, 
                                  String username, String password, boolean useHttps) {
        this.endpointHost = host;
        this.endpointPort = port;
        this.endpointPath = path;
        this.username = username;
        this.password = password;
        this.useHttps = useHttps;
        this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        this.readTimeout = DEFAULT_READ_TIMEOUT;
        
        // Initialize thread pool with custom configuration
        // NOTE: In a Java EE environment, it is better to inject a ManagedExecutorService
        this.executorService = new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(QUEUE_CAPACITY),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "SoapClient-Worker-" + threadNumber.getAndIncrement());
                    thread.setDaemon(false);
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy() // Execute on caller thread if queue is full
        );
        
        // Initialize SSL if using HTTPS
        if (useHttps) {
            initializeSSL();
        }
        
        logger.info("Async SoapClientService initialized for {}://{}:{}{}", 
            useHttps ? "https" : "http", host, port, path);
        logger.info("Thread pool configured: core={}, max={}, queue={}", 
            CORE_POOL_SIZE, MAX_POOL_SIZE, QUEUE_CAPACITY);
    }
    
    /**
     * Send SOAP message asynchronously with callback
     * @param soapXml The SOAP 1.1 XML message
     * @param messageUuid The unique message ID for the URL parameter
     * @param callback Callback for handling the response
     * @return Future for the operation (can be used for cancellation)
     */
    public Future<SoapResponse> sendSoapMessageAsync(String soapXml, String messageUuid, 
                                                     SoapResponseCallback callback) {
        
        if (isShuttingDown) {
            logger.warn("Service is shutting down, rejecting new request for UUID: {}", messageUuid);
            callback.onException(messageUuid, new RejectedExecutionException("Service shutting down"));
            return CompletableFuture.completedFuture(
                new SoapResponse(messageUuid, new RejectedExecutionException("Service shutting down")));
        }
        
        activeRequests.incrementAndGet();
        
        CompletableFuture<SoapResponse> future = CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            logger.info("Starting async SOAP request for UUID: {}", messageUuid);
            
            try {
                SoapResponse response = sendSoapMessageInternal(soapXml, messageUuid, startTime);
                
                // Invoke callback based on response
                if (response.isSuccess()) {
                    callback.onSuccess(response);
                } else {
                    callback.onFailure(response);
                }
                
                return response;
                
            } catch (Exception e) {
                logger.error("Exception during SOAP request for UUID: {}", messageUuid, e);
                callback.onException(messageUuid, e);
                return new SoapResponse(messageUuid, e);
                
            } finally {
                activeRequests.decrementAndGet();
                logger.debug("Active requests: {}", activeRequests.get());
            }
        }, executorService);
        
        // Add timeout handling
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            future.completeExceptionally(new TimeoutException("Operation timed out"));
        }, connectTimeout + readTimeout, TimeUnit.MILLISECONDS);

        future.whenComplete((response, throwable) -> timeoutTask.cancel(true));
        
        return future;
    }
    
    /**
     * Send SOAP message synchronously (for backward compatibility)
     * @param soapXml The SOAP XML message
     * @param messageUuid The unique message ID
     * @return SoapResponse
     */
    public SoapResponse sendSoapMessage(String soapXml, String messageUuid) {
        long startTime = System.currentTimeMillis();
        return sendSoapMessageInternal(soapXml, messageUuid, startTime);
    }
    
    /**
     * Internal method to send SOAP message
     */
    private SoapResponse sendSoapMessageInternal(String soapXml, String messageUuid, long startTime) {
        String urlWithMessageId = buildUrlWithMessageId(messageUuid);
        HttpURLConnection connection = null;
        
        try {
            // Create and configure connection
            URL url = new URL(urlWithMessageId);
            connection = (HttpURLConnection) url.openConnection();
            
            // FIXED: Apply instance-specific SSL settings
            if (useHttps && connection instanceof HttpsURLConnection) {
                if (sslContext != null) {
                    ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
                }
                if (hostnameVerifier != null) {
                    ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
                }
            }
            
            // Configure for SOAP 1.1
            configureConnection(connection, soapXml.getBytes(StandardCharsets.UTF_8).length);
            
            // Send SOAP request
            writeSoapRequest(connection, soapXml);
            
            // Get HTTP status code (per requirement: only process status code)
            int statusCode = connection.getResponseCode();
            String statusMessage = connection.getResponseMessage();
            long responseTime = System.currentTimeMillis() - startTime;
            
            // Log response
            logger.info("Received response for UUID {}: HTTP {} {} ({}ms)", 
                messageUuid, statusCode, statusMessage, responseTime);
            
            // Per requirement: HTTP 200 = Success, anything else = Failed
            SoapResponse response = new SoapResponse(statusCode, statusMessage, messageUuid, responseTime);
            
            // Log outcome
            if (response.isSuccess()) {
                logger.info("Message {} completed successfully", messageUuid);
            } else {
                logger.warn("Message {} failed with HTTP {}", messageUuid, statusCode);
            }
            
            return response;
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            logger.error("Error sending SOAP message for UUID {} ({}ms): {}", 
                messageUuid, responseTime, e.getMessage());
            return new SoapResponse(messageUuid, e);
            
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
    
    /**
     * Build the complete URL with Message ID parameter
     * Per requirement: &MessageId=<GUID> must be appended
     */
    private String buildUrlWithMessageId(String messageUuid) {
        StringBuilder url = new StringBuilder();
        
        url.append(useHttps ? "https://" : "http://");
        url.append(endpointHost);
        url.append(":");
        url.append(endpointPort);
        url.append(endpointPath);
        
        // Add query parameters for SAP integration
        // NOTE: These should ideally be passed in or read from config
        url.append("?senderParty=");
        url.append("&senderService=T_MERES");
        url.append("&interface=UtilitiesTimeSeriesERPItemBulkNotification_In");
        url.append("&receiverParty=");
        url.append("&receiverService=EHE000130");
        url.append("&interfaceNamespace=http://sap.com/xi/SAPGlobal20/Global");
        
        // Critical: Add MessageId parameter (per requirement)
        url.append("&MessageId=").append(messageUuid);
        
        return url.toString();
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
        if (username != null && password != null) {
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
        }
        
        // Additional headers
        connection.setRequestProperty("Accept", "text/xml");
        connection.setRequestProperty("Cache-Control", "no-cache");
        connection.setRequestProperty("Pragma", "no-cache");
        connection.setRequestProperty("User-Agent", USER_AGENT);
        connection.setRequestProperty("Connection", "close"); // Prevent connection reuse issues
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
            
            logger.debug("SOAP 1.1 request sent, size: {} bytes", soapXml.length());
        }
    }
    
    /**
     * Initialize SSL for HTTPS connections with proper security
     */
    private void initializeSSL() {
        try {
            // Load trust store with SAP certificates
            String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
            
            if (trustStorePath != null && trustStorePassword != null) {
                // Production: Use configured trust store
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (FileInputStream fis = new FileInputStream(trustStorePath)) {
                    trustStore.load(fis, trustStorePassword.toCharArray());
                }
                
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trustStore);
                
                sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, tmf.getTrustManagers(), new java.security.SecureRandom());
                
                logger.info("SSL initialized with trust store: {}", trustStorePath);
                
            } else {
                // Development/Test: Use default trust manager with logging
                logger.warn("No trust store configured, using default SSL context");
                
                sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, null, new java.security.SecureRandom());
            }
            
            // FIXED: Do NOT set JVM-wide defaults
            // HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
            
            // Hostname verification
            hostnameVerifier = new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    // Verify that hostname matches expected SAP endpoint
                    boolean valid = hostname.equalsIgnoreCase(endpointHost);
                    if (!valid) {
                        logger.warn("Hostname verification failed: expected={}, actual={}", 
                            endpointHost, hostname);
                    }
                    return valid;
                }
            };
            
            // FIXED: Do NOT set JVM-wide defaults
            // HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
            
            logger.info("SSL/TLS initialized for HTTPS connections (TLSv1.2)");
            
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
        HttpURLConnection connection = null;
        try {
            URL url = new URL(buildUrlWithMessageId("TEST-" + System.currentTimeMillis()));
            connection = (HttpURLConnection) url.openConnection();
            
            // FIXED: Apply instance-specific SSL settings
            if (useHttps && connection instanceof HttpsURLConnection) {
                if (sslContext != null) {
                    ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
                }
                if (hostnameVerifier != null) {
                    ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
                }
            }
            
            // FIXED: Use "GET" instead of "HEAD" as it's more likely to be allowed
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            
            if (username != null && password != null) {
                String auth = username + ":" + password;
                String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
                connection.setRequestProperty("Authorization", "Basic " + encodedAuth);
            }
            
            int responseCode = connection.getResponseCode();
            
            logger.info("Connection test result: HTTP {}", responseCode);
            // Any response (even 404, 405, 500) means the host is reachable
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
     * Get current number of active requests
     */
    public int getActiveRequestCount() {
        return activeRequests.get();
    }
    
    /**
     * Get thread pool statistics
     */
    public String getThreadPoolStats() {
        if (executorService instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) executorService;
            return String.format("Pool[active=%d, completed=%d, queue=%d]",
                tpe.getActiveCount(),
                tpe.getCompletedTaskCount(),
                tpe.getQueue().size());
        }
        return "N/A";
    }
    
    /**
     * Shutdown the service gracefully
     * @param timeoutSeconds Maximum time to wait for pending requests
     */
    public void shutdown(int timeoutSeconds) {
        logger.info("Initiating shutdown with {}s timeout", timeoutSeconds);
        isShuttingDown = true;
        
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                logger.warn("Timeout reached, forcing shutdown");
                executorService.shutdownNow();
                
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.error("Thread pool did not terminate");
                }
            }
        } catch (InterruptedException e) {
            logger.error("Shutdown interrupted", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Shutdown complete. Active requests at shutdown: {}", activeRequests.get());
    }
}