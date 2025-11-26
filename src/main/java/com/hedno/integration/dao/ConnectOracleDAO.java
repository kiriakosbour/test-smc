package com.hedno.integration.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;


public class ConnectOracleDAO {
	public Connection conn = null;
	public DataSource dataSource = null;

	public PreparedStatement ps = null;
	public ResultSet rs = null;
	public Statement stmt = null;

	static String driver;
	static String connectString;
	static String usr; 
	static String pass;

	public ConnectOracleDAO(Properties props){

		try{
			initializeDataSource(props.getProperty("jndi.datasource.name", "jdbc/artemis_smc"));
			if (dataSource == null) {
				driver = props.getProperty("db.driver");
				connectString = props.getProperty("db.url");
				usr = props.getProperty("db.username");  
				pass = props.getProperty("db.password");
				System.out.println("ConnectOracle Constuctor Called");
			}
		}catch (Exception e) {
			System.out.println("Constuctor Exception");
			e.printStackTrace();
			try{
				throw new Exception();
			}
			catch (Exception e1) {
				System.out.println("Constuctor 2nd Exception");
				e1.printStackTrace();
			}
		}
	}


	private void initializeDataSource(String dsName) {
		try {
			InitialContext ctx = new InitialContext();
			dataSource = (DataSource) ctx.lookup(dsName);
		} catch (Exception e) {
			System.out.println("Initialize datasource exception");
			e.printStackTrace();
		}
	}



	private Connection getConnectionInternal(){
		try {
			System.out.println("static ConnectOracleDAO.getConnection() Called");
			Class.forName(driver);//19c
			//Get Connection
			Connection connRet = DriverManager.getConnection(connectString, usr, pass);
			//conn = connRet;

			return connRet;
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}    

	public Connection getConnection(){
		try {
			if (dataSource != null) {
				return dataSource.getConnection();
			} else {
				return getConnectionInternal();
			}
		} catch (Exception e) {
			System.out.println("getConnection datasource exception");
			e.printStackTrace();
		}
		return null;
	}   

	// this is better to be removed
    public PreparedStatement getPreparedStatement(Connection conn, String sql){
        try {
            if (dataSource != null) {
                ps = conn.prepareStatement(sql);
            } else {
                ps = conn.prepareStatement(sql);
            }
            return ps;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
	 
}
