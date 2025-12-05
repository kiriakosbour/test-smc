package com.hedno.integration.sftp;

//import com.jcraft.jsch.Channel;
import com.hedno.integration.service.SFTPDownloadFileService;
import com.hedno.integration.service.XMLReaderService;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import com.jcraft.jsch.SftpException;
//import java.util.Properties;
//import static com.jcraft.jsch.JSch.setConfig;
//import static com.jcraft.jsch.JSch.setConfig;
//JSch.setConfig("StrictHostKeyChecking", "no");
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.util.Properties.*;

public class Itron {
    private static final Logger logger = LoggerFactory.getLogger(Itron.class);
    private static final Properties properties = new Properties();
    static {
        try ( InputStream input = XMLReaderService.class.getClassLoader().getResourceAsStream("config/application.properties") ) {

            if (input == null) {
                throw new RuntimeException("Cannot find application.properties on classpath");
            }

            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading application.properties", e);
        }
    }
    public static String get(String key) {
        return properties.getProperty(key);
    }
    //Itron 10.138.19.133

    //https://www.sftp.net/public-online-ftp-servers

    //https://www.rebex.net/doc/buru-sftp-server/getting-started/02-users/
    //127.0.0.1 geo geo

    // test.rebex.net  194.108.117.16  demo  password
    //test ftp.dlptest.com 44.241.66.173  User: dlpuser Password: rNrKYTX9g7z3RgJRmxWuGHbeu
    //https://www.wftpserver.com/onlinedemo.htm
    /*
    private String remoteHost = System.getProperty("SFTP.host","127.0.0.1");
    private Integer remotePort = Integer.parseInt(System.getProperty("SFTP.port","22"));
    private String username = System.getProperty("SFTP.username","geo");
    private String password = System.getProperty("SFTP.password","geo");
    */

    private String remoteHost = get("sftp.host");
    private Integer remotePort = Integer.parseInt(get("sftp.port"));
    private String username = get("sftp.username");
    private String password = get("sftp.password");

    /*
    private String remoteHost = "10.138.19.133";
    private int remotePort = 22;
    private String username = "";
    private String password = "";
    */
    public ChannelSftp setupJsch() throws JSchException {
        try {
            JSch jsch = new JSch();

            jsch.setConfig("StrictHostKeyChecking", "no");

            //Properties config = new java.util.Properties();
            // config.put("StrictHostKeyChecking", "no");
            //jsch.setKnownHosts("/Users/john/.ssh/known_hosts");
            logger.info("----->Connecting to " + remoteHost + ":" + remotePort);
            Session jschSession = jsch.getSession(username, remoteHost, remotePort);
            //jschSession.setConfig(config);
            jschSession.setPassword(password);
            jschSession.connect();

            return (ChannelSftp) jschSession.openChannel("sftp");
        }catch (JSchException e){
            logger.error(e.getMessage());
            return null;
        }
        catch (Exception e){
            logger.error(e.getMessage());
            return null;
        }
    }
}
