package com.hedno.integration.service;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import java.io.*;
import java.sql.*;

import com.hedno.integration.ConfigService;

import com.hedno.integration.sftp.Itron;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.JSchException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;//interface
import java.util.concurrent.TimeUnit;

import java.util.Vector;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Getting data from SFTP SEVER.
 * This is used when we need to parse XML data to store into the database.
 *
 * @author HEDNO Integration Team
 * @version 1.0
 */
public class SFTPDownloadFileService implements ServletContextListener {
    private static final Logger logger = LoggerFactory.getLogger(SFTPDownloadFileService.class);

    //static final Logger logger = LoggerFactory.getLogger(SFTPDownloadFileService.class);

    public static ChannelSftp channelSftp = null;
    public static String backUpRemoteDir = "";
    public static String fileExt = ".XML";
    public static String remoteSearchRootPath = ".";
    public static String remoteSearchFile = "file_template.xml";
    public static int numFilesRead = 0;
    //private static final String INSERT_SQL_CLOB = "INSERT INTO ITRON_FILE_PROCESS (F_ID,F_NAME,F_CONTENT) values (?, ?, ?)";

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     *
     * @param arg0
     */
    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        try {
            logger.info("=============>SFTPDownloadFileService.contextInitialized()");
            int schHours    = Integer.parseInt(ConfigService.get("sch.hours"));
            int schMinutes  = Integer.parseInt( ConfigService.get("sch.minutes") );
            int schSeconds  = Integer.parseInt(ConfigService.get("sch.seconds") );
            startExecution(schHours,schMinutes,schSeconds);
        } catch (Exception e) {
            logger.error("=====>Error In SFTPDownloadFileService.contextInitialized() :" + e.getMessage());
        }
    }

    public void startExecution(int targetHour, int targetMin, int targetSec){
        Runnable taskSchedule =  () -> {
            try {
                logger.info("====>Start Thread.initiateProcess() :"  +  new Date() );
                initiateProcess();
                logger.info("====>About to startExecution :"  +  new Date() );
                startExecution(0,targetMin,0);
            } catch (Exception e) {
                logger.error("=====>Error In Thread.initiateProcess() :" + e.getMessage());
            }
        };
        scheduler.schedule(taskSchedule, targetMin, TimeUnit.MINUTES);
    }

    public static int connectSFTPServer() throws JSchException {

        Itron objItron = new Itron();
        try {
            channelSftp = objItron.setupJsch();
            channelSftp.connect();
            //channelSftp.put(localFile, remoteDir + "jschFile.txt");
            //channelSftp.get(remoteFile, localDir + "jschFile.txt");
            logger.info("shell channel connected.... " + channelSftp.isConnected());

            //channelSftp.lcd(localDir);
            logger.info("channelSftp.pwd() :" + channelSftp.pwd());
            return 1;
        } catch (SftpException e) {
            //throw new RuntimeException(e);
            e.printStackTrace();
            return -1;
        } catch (Exception e) {
            //throw new RuntimeException(e);
            e.printStackTrace();
            return -2;
        }
    }

    public static void downloadFiles(String remotePath) throws JSchException, IOException {

        try {
            //Formatting date for Backup
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();

            // Get a listing  of the remote directory (ls) for Specific file
            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls(remoteSearchRootPath + "/" + remotePath + "/" + fileSearchPattern);
            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls("./pub/example");

            Vector<ChannelSftp.LsEntry> lstFiles = channelSftp.ls(remoteSearchRootPath + "/" + remotePath );
            //Vector<ChannelSftp.LsEntry> lstFiles = channelSftp.ls(remoteSearchRootPath  );
            //Vector<ChannelSftp.LsEntry> lstFiles = channelSftp.ls(remoteSearchRootPath + "/" + "sftp/Export" );

            numFilesRead = 0;
            //Find number of Files , Excluding Directories and . , ..
            for( int i=0;  i < lstFiles.size(); i++ ) {
                ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) lstFiles.get(i);
                logger.info(i + " entry.getFilename() :" + entry.getFilename() + " isDir:" + entry.getAttrs().isDir());
                if ( ".".equals(entry.getFilename()) || "..".equals(entry.getFilename()) || entry.getAttrs().isDir() ) {
                    continue;
                }
                numFilesRead++;
            }
            logger.info("In remote Path :" + remotePath + " FilesRead :" + numFilesRead);
            // iterate through objects in list, identifying specific file names

            //Create BackUp directories Only when files found
            if (numFilesRead  != 0) {
                backUpRemoteDir =  dtf.format(now).toString().replace('/','_').replace(' ','_').replace(':','_');
                channelSftp.mkdir(remoteSearchRootPath + "/" + remotePath + "/" + backUpRemoteDir);
            }
            for (ChannelSftp.LsEntry oListItem : lstFiles) {
                //logger.info(oListItem.toString());
                //logger.info(oListItem.toString() + " isDirectory:" + oListItem.getAttrs().isDir());

                //check if it is NOT a directory
                if (!oListItem.getAttrs().isDir()) {
                    //logger.info("get :" + oListItem.getFilename());
                    if (oListItem.getFilename().toUpperCase().contains(fileExt)) {
                        //channelSftp.get(remoteSearchRootPath + "/pub/example/" + oListItem.getFilename(), oListItem.getFilename());
                        channelSftp.get(remoteSearchRootPath + "/" + remotePath + "/" + oListItem.getFilename(), oListItem.getFilename());
                        logger.info("get :" + oListItem.getFilename());

                        //file Will Be retrieved from SFTP server
                        //Going to be processed(validation of XML structure and possible fixed) and loaded with detailed Data into DB
                        int processReadingXML = -11;
                        int processAlarmsXML = -11;
                        int processEventsXML = -11;

                        switch (remotePath) {
                            case "sftp/Export/Reading":
                                processReadingXML = XMLReaderService.processReadingsXML(oListItem.getFilename());
                                logger.info("processReadingXML :" + processReadingXML);
                                break;
                            case "sftp/Export/Alarms":
                                processAlarmsXML = XMLReaderService.processAlarmsXML(oListItem.getFilename());
                                logger.info("processAlarmsXML :" + processAlarmsXML);
                                break;
                            case "sftp/Export/Events":
                                processEventsXML = XMLReaderService.processEventsXML(oListItem.getFilename());
                                logger.info("processEventsXML :" + processEventsXML);
                                break;
                        }
                        /*
                        logger.info("File :" + oListItem.getFilename() + " to be del from Current dir :::::::::::::::::::::::" + new java.io.File(".").getCanonicalPath());
                        String delCommand = "cmd.exe /c scr_Del.cmd " + oListItem.getFilename();
                        Runtime.getRuntime().exec("del " + oListItem.getFilename());
                        */
                        //file retrieved from SFTP server , going to be Loaded as CLOB into DB
                        //int processLoadCLOBIntoDB = -11;
                        //processLoadCLOBIntoDB = loadCLOBIntoDB(XMLReaderService.objConn.getPreparedStatement(INSERT_SQL_CLOB) , oListItem.getFilename()  );
                        //logger.info("processLoadCLOBIntoDB :" + processLoadCLOBIntoDB);

                        if (processReadingXML == 0 || processAlarmsXML == 0 || processEventsXML == 0) {
                            //0:success
                            channelSftp.put(oListItem.getFilename(), remoteSearchRootPath + "/" + remotePath + "/" + backUpRemoteDir);
                            channelSftp.rm(remoteSearchRootPath + "/" + remotePath + "/" + oListItem.getFilename());
                        }
                        //Delete file
                        File file = new File(oListItem.getFilename());
                        if (file.delete()) {
                            logger.info(oListItem.getFilename() + " deleted successfully from " + new java.io.File(".").getCanonicalPath());
                        } else {
                            logger.info(oListItem.getFilename() + " failed to be deleted");
                        }

                    }
                }
            }
        } catch (SftpException e) {
            throw new RuntimeException(e);
        } finally {
            // disconnect session.  If this is not done, the job will hang and leave log files locked
            //session.disconnect();
        }
    }

    public static int loadCLOBIntoDB(PreparedStatement ps , String in_xmlFile) {
        try {
            long startTime = System.currentTimeMillis();
            long totalDuration;

            // Works much like the FileInputStream except the FileInputStream reads bytes, whereas the FileReader reads characters.
            // The FileReader is intended to read text, in other words. One character may correspond to one
            // or more bytes depending on the character encoding scheme.     Charset.forName("UTF8")
            Reader fileReader  = new FileReader(in_xmlFile);
            /*
            int data = fileReader.read();
            while(data != -1) {
                //do something with data...
                data = fileReader.read();
            }
             */
            ps.setInt(1, 3);                // your id value
            ps.setString(2, in_xmlFile);      // optional filename
            // stream the CLOB (length unknown is fine)
            ps.setCharacterStream(3, fileReader);

            ps.executeUpdate();

            logger.info(in_xmlFile + " inserted as CLOB successfully .");

            totalDuration = System.currentTimeMillis() - startTime;
            logger.info("Duration :" + (totalDuration / 1000) / 60 + " Mins");
            logger.info("Duration :" + (totalDuration / 1000) + " Secs");
            logger.info("Duration :" + totalDuration + " Millisecs");

            fileReader.close();
            return 1;
        }catch (Exception ex) {
            logger.info("An error occurred: " + ex.getMessage());
            return -1;
        }
    }

    public static void initiateProcess() throws JSchException {
        try {
            //Connect to SFTP Server
            int resultConnect = connectSFTPServer();
            logger.info("resultConnect :" + resultConnect);
            if (resultConnect == 1) {
                try {
                    //DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    //LocalDateTime now = LocalDateTime.now();

                    //following commands throw SftpException e
                    //backUpRemoteDir =  dtf.format(now).toString().replace('/','_').replace(' ','_').replace(':','_');
                    //channelSftp.mkdir(remoteSearchPath + "/" + backUpRemoteDir);

                    //----------------Execute Main Operation for each Path
                    //downloadFiles("sftp/Export/AMI_DaReadintaExport/g");
                    //downloadFiles("sftp/Export/AMI_DataExport");
                    /*
                    downloadFiles("/sftp/Export/Reading");
                    downloadFiles("/sftp/Export/Alarms");
                    downloadFiles("/sftp/Export/Events");
                    */
                    //logger.info("========>Search Remote Dir :" + ConfigService.get("sftp.fldr.readings") );
                    downloadFiles(ConfigService.get("sftp.fldr.readings"));
                    downloadFiles(ConfigService.get("sftp.fldr.events"));
                    downloadFiles(ConfigService.get("sftp.fldr.alarms"));
                }
                /*
                catch(SftpException sftpe){
                    logger.info("sftpe.id:" + sftpe.id + " message:" + sftpe.getMessage() );
                    if (sftpe.getMessage().contains("Already exists") && sftpe.id == 4 ){
                    }
                    else {
                        throw new Exception();
                    }
                }
                */
                catch(Exception e){
                    logger.error(e.getMessage());
                }
            }
        } catch(Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }  finally {
            if (channelSftp != null){
                channelSftp.exit();
                channelSftp.disconnect();
            }
            logger.info("channelSftp.exit()");
        }
    }
}
