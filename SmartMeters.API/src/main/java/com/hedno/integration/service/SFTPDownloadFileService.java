package com.hedno.integration.service;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.*;

import com.hedno.integration.dao.IntervalDataDAO;
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

import java.nio.charset.StandardCharsets;


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

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        try {
            logger.info("=============>SFTPDownloadFileService.contextInitialized()");
            startExecution(0,5,0);
        } catch (Exception e) {
            System.out.println("=====>Error In SFTPDownloadFileService.contextInitialized() :" + e.getMessage());
        }
    }

    public void startExecution(int targetHour, int targetMin, int targetSec){
        Runnable taskSchedule =  () -> {
            try {
                System.out.println("====>About to Thread.initiateProcess() :"  +  new Date() );
                initiateProcess();
                System.out.println("====>About to startExecution :"  +  new Date() );
                startExecution(0,targetMin,0);
            } catch (Exception e) {
                System.out.println("=====>Error In Thread.initiateProcess() :" + e.getMessage());
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
            System.out.println("shell channel connected.... " + channelSftp.isConnected());

            //channelSftp.lcd(localDir);
            System.out.println("channelSftp.pwd() :" + channelSftp.pwd());
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
            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls(remoteSearchRootPath);
            Vector<ChannelSftp.LsEntry> list = channelSftp.ls(remoteSearchRootPath + "/" + remotePath + "/");

            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls(remoteSearchRootPath  + " " + remoteSearchFile);
            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls(remoteSearchFile);


            //Vector<ChannelSftp.LsEntry> list = channelSftp.ls("./pub/example");

            numFilesRead = list.size();
            System.out.println("list.size() :" + numFilesRead);
            // iterate through objects in list, identifying specific file names

            //Create BackUp directories Only when files found
            //-2 is used Because . and .. are Automatically Created
            if (numFilesRead - 2 != 0) {
                backUpRemoteDir =  dtf.format(now).toString().replace('/','_').replace(' ','_').replace(':','_');
                channelSftp.mkdir(remoteSearchRootPath + "/" + remotePath + "/" + backUpRemoteDir);
            }
            for (ChannelSftp.LsEntry oListItem : list) {
                //logger.info(oListItem.toString());
                //System.out.println(oListItem.toString() + " isDirectory:" + oListItem.getAttrs().isDir());

                //check if it is NOT a directory
                if (!oListItem.getAttrs().isDir()) {
                    //System.out.println("get :" + oListItem.getFilename());
                    if (oListItem.getFilename().toUpperCase().contains(fileExt)) {
                        //channelSftp.get(remoteSearchRootPath + "/pub/example/" + oListItem.getFilename(), oListItem.getFilename());
                        channelSftp.get(remoteSearchRootPath + "/" + remotePath + "/"  + oListItem.getFilename(), oListItem.getFilename());
                        System.out.println("get :" + oListItem.getFilename());

                        //file Will Be retrieved from SFTP server
                        //Going to be processed(validation of XML structure and possible fixed) and loaded with detailed Data into DB
                        int processReadingsXML = -11;
                        int processAlarmsXML = -11;
                        int processEventsXML = -11;

                        switch (remotePath) {
                            case "readings":
                                processReadingsXML = XMLReaderService.processReadingsXML(oListItem.getFilename());
                                System.out.println("processReadingsXML :" + processReadingsXML);
                                break;
                            case "alarms":
                                processAlarmsXML = XMLReaderService.processAlarmsXML(oListItem.getFilename());
                                System.out.println("processAlarmsXML :" + processAlarmsXML);
                                break;
                            case "events":
                                processEventsXML = XMLReaderService.processEventsXML(oListItem.getFilename());
                                System.out.println("processEventsXML :" + processEventsXML);
                                break;
                        }
                        /*
                        System.out.println("File :" + oListItem.getFilename() + " to be del from Current dir :::::::::::::::::::::::" + new java.io.File(".").getCanonicalPath());
                        String delCommand = "cmd.exe /c scr_Del.cmd " + oListItem.getFilename();
                        Runtime.getRuntime().exec("del " + oListItem.getFilename());
                        */
                        //file retrieved from SFTP server , going to be Loaded as CLOB into DB
                        //int processLoadCLOBIntoDB = -11;
                        //processLoadCLOBIntoDB = loadCLOBIntoDB(XMLReaderService.objConn.getPreparedStatement(INSERT_SQL_CLOB) , oListItem.getFilename()  );
                        //System.out.println("processLoadCLOBIntoDB :" + processLoadCLOBIntoDB);

                        if ( processReadingsXML == 0 || processAlarmsXML == 0 || processEventsXML == 0 ) {
                            //0:success
                            channelSftp.put(oListItem.getFilename(),remoteSearchRootPath + "/" + remotePath+ "/"+ backUpRemoteDir  );
                            channelSftp.rm(remoteSearchRootPath + "/" + remotePath + "/" + oListItem.getFilename() );
                        }
                        //Delete file
                        File file = new File(oListItem.getFilename());
                        if (file.delete()) {
                            System.out.println(oListItem.getFilename() + " deleted successfully");
                        }
                        else {
                            System.out.println(oListItem.getFilename() + " failed to be deleted");
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


            System.out.println(in_xmlFile + " inserted as CLOB successfully .");

            totalDuration = System.currentTimeMillis() - startTime;
            System.out.println("Duration :" + (totalDuration / 1000) / 60 + " Mins");
            System.out.println("Duration :" + (totalDuration / 1000) + " Secs");
            System.out.println("Duration :" + totalDuration + " Millisecs");

            fileReader.close();
            return 1;
        }catch (Exception ex) {
            System.out.println("An error occurred: " + ex.getMessage());
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

                    //Execute Main Operation for each Path
                    downloadFiles("readings");
                    downloadFiles("events");
                    downloadFiles("alarms");
                }
                /*
                catch(SftpException sftpe){
                    System.out.println("sftpe.id:" + sftpe.id + " message:" + sftpe.getMessage() );
                    if (sftpe.getMessage().contains("Already exists") && sftpe.id == 4 ){
                    }
                    else {
                        throw new Exception();
                    }
                }
                */
                catch(Exception e){
                    System.out.println(e.getMessage());
                }

            }
        } catch(Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }  finally {
            channelSftp.exit();
            System.out.println("channelSftp.exit()");
        }
    }
}
