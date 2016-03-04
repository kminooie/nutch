package org.apache.nutch.parse.html_a;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.mariadb.jdbc.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectMysql implements Knowledge {


	

	private static Connection conn = null;

	public static  PreparedStatement psSelect = null;
	public static  PreparedStatement psInsert = null;
	public static  PreparedStatement psUpdate = null;
	
	public static final Logger LOG = LoggerFactory.getLogger( ConnectMysql.class );

	//constructor 
	public ConnectMysql(){
		// TODO read db credentials from nutch conf object 
				
		initConnection();
	}

	private static boolean initConnection() {
		
		
		boolean result = true;

		if( null == conn ) {
			try {
				
				Class.forName("org.mariadb.jdbc.Driver");
				String sqlConnection="jdbc:mysql://"+TrainingPart.domain_2locos_tariningpart+"/"+TrainingPart.schema_2locos_tariningpart;
				conn = DriverManager.getConnection(sqlConnection, TrainingPart.USER_2locos_tariningpart, TrainingPart.PASS_2locos_tariningpart);
				LOG.info("conection successfully stablished" );
			} catch( Exception e ) {
				LOG.error( "Failed to establish connection:", e );
				result = false;
			}
		}
		
		return result;
	}


	@Override
	public boolean addIncNode(String domain, String path, String xpath, String content) {
		

		boolean result = addNode( domain, path, xpath, content ); 
		
		if( ! result ) {
			result = incNodeFreq( domain, xpath, content.hashCode() );
		}
		
		return result;
	}

	//this function add a node in database  done!
	@Override
	public boolean addNode(String domain, String path, String xpath, String content) {

		boolean result = false;
		
		
		try {
			
			if( null == psSelect ) {
				psSelect = conn.prepareStatement( "INSERT INTO pages (host,xpath,hash,frequency,content,path) VALUES (?, ?, ?, 1, ?, ?);" );
				LOG.info("insert a node in database. the node is : "+xpath);
			}


			psSelect.setString(1,domain);
			psSelect.setString(2,xpath);
			psSelect.setInt(3, content.hashCode());
			psSelect.setString(4,content);
			psSelect.setString(5,path);

			psSelect.executeUpdate();
			result = true;
		} catch( java.sql.SQLIntegrityConstraintViolationException e ) {
			// assuming node already exist 
			// TODO make sure that is true
			LOG.info( "Got SQLIntegrity exception assume node already exist hash:" + xpath );
			result = false; // Redundant
		
		}catch( java.sql.BatchUpdateException e ) {
			// assuming node already exist 
			// TODO make sure that is true

			result = false; // Redundant
		
		} catch (SQLException e) {
			// TODO check for existing node is part of normal operation and not an error
			LOG.error( "Exception while adding a new node:", e );
		}

		return result;
	}



	//just increase the frequency field of a node  done!
	@Override
	public boolean incNodeFreq(String domain, String xpath, int hash) {
		boolean result = false;
		

		
		try {
			
			if( null == psUpdate ) {
				psUpdate = conn.prepareStatement( "update pages set frequency=frequency+1  where host= ? and xpath= ? and hash= ? ;" );
			}

			psUpdate.setString(1,domain);
			psUpdate.setString(2,xpath);
			psUpdate.setInt(3,hash);

			psUpdate.executeUpdate();
			
			result = true;
		} catch (SQLException e) {
			LOG.error( "Exception while increasing the frequency:", e );
		}


		return result;
	}

	@Override
	public Map<String, Object> getNode(String domain, String xpath, int hash) {
		// TODO "select hash from pages where host= ? and xpath= ? and content= ?;"
		return null;
	}


	//use this function to check the connection at the end and if the connections were open this function close them.

	protected void finalize(){
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn = null;
		}
	}


}




