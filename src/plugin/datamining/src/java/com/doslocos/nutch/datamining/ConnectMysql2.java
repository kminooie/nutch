package com.doslocos.nutch.datamining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectMysql2 implements Knowledge {

	public static String Schema_2locos_tariningpart;
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static String Host_2locos_tariningpart;

	private static Connection conn = null;
	public static  PreparedStatement psNode, psUrl, psFrequency, psGetFrequency;

	public static final Logger LOG = LoggerFactory.getLogger( ConnectMysql.class );

	
	public static int counter = 0;
	
	public static void resetCounter() {
		counter = 0;
	}
	
	public static void incCounter() {
		++counter;
	}
	
	public static int getCounter() {
		return counter;
	}

	//constructor 
	public ConnectMysql2(String schema2locos,String host2locos,String pass2locos,String user2locos){
		// TODO read db credentials from nutch conf object 

		Schema_2locos_tariningpart=schema2locos;
		USER_2locos_tariningpart =user2locos;
		PASS_2locos_tariningpart=pass2locos;
		Host_2locos_tariningpart=host2locos;

		LOG.debug("Connection class called");
		initConnection( false );
	}


	private static void checkConnection() {
		boolean renew = false;
		try {
			if ( conn.isClosed() ) renew = true;
		} catch ( Exception e ) {
			LOG.error( "Exception while trying to check connection:", e );
			renew = true;
		}

		if( renew ) {
			renew = initConnection( true );
			LOG.info("kaveh, checkConnection called initConnection");

		}

		if( ! renew ) {
			// TODO die here
		}
	}

	private static boolean initConnection( boolean force ) {
		if( force || null == conn ) {
			try {
				LOG.debug("Connection to database stablished");
				Class.forName("org.mariadb.jdbc.Driver");
				String sqlConnection="jdbc:mysql://"+Host_2locos_tariningpart+"/"+Schema_2locos_tariningpart;
				conn = DriverManager.getConnection(sqlConnection, USER_2locos_tariningpart, PASS_2locos_tariningpart);
			} catch( Exception e ) {
				LOG.error( "Failed to establish connection:", e );
				return false;
			}
		}

		return true;
	}



	@Override
	public boolean addIncNode(String domain, String path, String xpath, String content) {

		return addNode( domain, path, xpath, content ); 

	}



	//this function add a node in database  done!
	@Override
	public boolean addNode(String domain, String path, String xpath, String content) {
		long nodeId = 0, urlId = 0;
		boolean result = false;
		
		LOG.info( "adding domain:"+domain+" path:"+path+" xpath:"+xpath );
		checkConnection();
		try {

			if( null == psNode ) {
				psNode = conn.prepareStatement( 
					"INSERT INTO nodes (host,xpath,hash) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
					, Statement.RETURN_GENERATED_KEYS
				);
			}

			psNode.setString(1,domain);
			psNode.setString(2,xpath);
			psNode.setInt(3, content.hashCode());
			
			nodeId = psNode.executeUpdate();			
			LOG.info( "nodeId:"+nodeId );
			incCounter();
			
			if( null == psUrl ) {
				psUrl = conn.prepareStatement( 
					"INSERT INTO urls (host,path) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
					, Statement.RETURN_GENERATED_KEYS
				);
			}

			psUrl.setString(1,domain);
			psUrl.setString(2,path);
			
			urlId = psUrl.executeUpdate();    
			LOG.info( "urlId:"+urlId );
			incCounter();
			
			if( null == psFrequency ) {
				psFrequency = conn.prepareStatement( "INSERT IGNORE INTO frequency( node_id,url_id ) VALUES (?, ?);" );
			}

			psFrequency.setLong( 1, nodeId );
			psFrequency.setLong( 2, urlId );
			
			psFrequency.executeUpdate();    
			
			incCounter();
			
			result = true;
		} catch( java.sql.SQLIntegrityConstraintViolationException e ) {

			LOG.error( "Got SQLIntegrity exception assume node already exist hash:" + xpath );
			return result;

		}catch( java.sql.BatchUpdateException e ) {
			LOG.error( "alireza, Got BatchUpdateException exception assume node already exist hash:" + xpath );
			return result;

		} catch (SQLException e) {
			// TODO check for existing node is part of normal operation and not an error
			LOG.error( "Exception while adding a new node:", e );
			return result;
		}

		return result;
	}



	//just increase the frequency field of a node  done!
	@Override
	public boolean incNodeFreq(String domain, String xpath, int hash) {

		boolean result = false;
		return result;
	}



	@Override
	public int getNodeFreq( String domain,String xpath, String content ) {
		int result = 0;


		checkConnection();
		try {	

			psGetFrequency=conn.prepareStatement( "SELECT count(url_id ) FROM nodes JOIN frequency ON ( node_id = id ) WHERE host=? AND xpath=? AND hash=? ;" );
			psGetFrequency.setString(1,domain);
			psGetFrequency.setString(2, xpath);
			psGetFrequency.setInt(3,content.hashCode());

			ResultSet rs=psGetFrequency.executeQuery();

			if(rs.next()) {
				result = rs.getInt( 0 );
			}else{
				LOG.debug( "Node " + xpath + " is not in database. and content is: "+ content );
			}
			
			incCounter();

		} catch (SQLException e) {

			LOG.error("Except During compare a node with database :"+xpath+"  "+e);

		}


		return result;
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

