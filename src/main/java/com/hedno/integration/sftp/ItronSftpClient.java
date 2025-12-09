package com.hedno.integration.sftp;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * ITRON SFTP Client wrapper.
 * Handles connection, authentication, and channel management.
 * 
 * @author HEDNO Integration Team
 * @version 3.0
 */
public class ItronSftpClient {

    private static final Logger logger = LoggerFactory.getLogger(ItronSftpClient.class);

    private static final int DEFAULT_TIMEOUT = 30000;
    private static final int DEFAULT_PORT = 22;

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final int timeout;

    private Session session;
    private ChannelSftp channel;

    /**
     * Constructor with Properties configuration
     */
    public ItronSftpClient(Properties props) {
        this.host = props.getProperty("sftp.host", "localhost");
        this.port = Integer.parseInt(props.getProperty("sftp.port", String.valueOf(DEFAULT_PORT)));
        this.username = props.getProperty("sftp.username", "");
        this.password = props.getProperty("sftp.password", "");
        this.timeout = Integer.parseInt(props.getProperty("sftp.timeout", 
            String.valueOf(DEFAULT_TIMEOUT)));
    }

    /**
     * Constructor with explicit parameters
     */
    public ItronSftpClient(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.timeout = DEFAULT_TIMEOUT;
    }

    /**
     * Connect to SFTP server
     */
    public void connect() throws JSchException {
        logger.info("Connecting to SFTP server: {}@{}:{}", username, host, port);

        JSch jsch = new JSch();

        // Create session
        session = jsch.getSession(username, host, port);
        session.setPassword(password);

        // Configure session
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        config.put("PreferredAuthentications", "password");
        session.setConfig(config);
        session.setTimeout(timeout);

        // Connect session
        session.connect(timeout);
        logger.debug("SSH session connected");

        // Open SFTP channel
        channel = (ChannelSftp) session.openChannel("sftp");
        channel.connect(timeout);

        logger.info("SFTP connection established to {}:{}", host, port);
    }

    /**
     * Disconnect from SFTP server
     */
    public void disconnect() {
        if (channel != null && channel.isConnected()) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                logger.debug("Error disconnecting channel", e);
            }
        }

        if (session != null && session.isConnected()) {
            try {
                session.disconnect();
            } catch (Exception e) {
                logger.debug("Error disconnecting session", e);
            }
        }

        logger.debug("SFTP disconnected from {}:{}", host, port);
    }

    /**
     * Get the SFTP channel
     */
    public ChannelSftp getChannel() {
        return channel;
    }

    /**
     * Check if connected
     */
    public boolean isConnected() {
        return session != null && session.isConnected() &&
               channel != null && channel.isConnected();
    }

    /**
     * Get current working directory
     */
    public String pwd() throws SftpException {
        if (channel == null || !channel.isConnected()) {
            throw new SftpException(ChannelSftp.SSH_FX_NO_CONNECTION, "Not connected");
        }
        return channel.pwd();
    }

    /**
     * Change directory
     */
    public void cd(String path) throws SftpException {
        if (channel == null || !channel.isConnected()) {
            throw new SftpException(ChannelSftp.SSH_FX_NO_CONNECTION, "Not connected");
        }
        channel.cd(path);
    }

    /**
     * Get host info for logging
     */
    public String getHostInfo() {
        return username + "@" + host + ":" + port;
    }
}
