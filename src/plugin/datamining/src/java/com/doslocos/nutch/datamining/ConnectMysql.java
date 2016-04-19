package com.doslocos.nutch.datamining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectMysql extends Knowledge {

	private static String SCHEMA;
	private static String USER;
	private static String PASS;
	private static String DBHOST;

	private static Connection conn = null;
	private static  PreparedStatement psHost, psNode, psUrl, psFrequency, psGetFrequency, psgetfreq;
	private ResultSet tempRs = null;

	private static final Map< String, Integer > hostIds = new ConcurrentHashMap< String , Integer>();

	public static final Logger LOG = LoggerFactory.getLogger( ConnectMysql.class );


	public ConnectMysql( Configuration conf ) {
		DBHOST = conf.get("doslocos.training.database.host");
		SCHEMA = conf.get("doslocos.training.database.schema");
		USER = conf.get("doslocos.training.database.username");
		PASS = conf.get("doslocos.training.database.password");

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
			renew = ! initConnection( true );
			LOG.debug( "Renewed database connection.");
		}

		if( renew ) {
			LOG.error( "Unable to renew the database connection." );
			// die here
		}


	}

	private static boolean initConnection( boolean force ) {
		if( force || null == conn ) {
			try {
				LOG.debug("Connection to database stablished");
				Class.forName("org.mariadb.jdbc.Driver");
				String sqlConnection="jdbc:mysql://" + DBHOST + "/" + SCHEMA;
				conn = DriverManager.getConnection(sqlConnection, USER, PASS );
			} catch( Exception e ) {
				LOG.error( "Failed to establish connection:", e );
				return false;
			}
		}

		return true;
	}


	@Override
	public int getHostId( String domain ) {
		Integer result = 0;

		result = hostIds.get( domain );
		if( null == result ) {
			checkConnection();
			++counter;
			try {
				if( null == psHost ) {
					psHost = conn.prepareStatement( 
							"INSERT INTO hosts ( domain ) VALUES ( ? ) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
							, Statement.RETURN_GENERATED_KEYS
							);
				}

				psHost.setString(1,domain);
				psHost.executeUpdate();
				tempRs = psHost.getGeneratedKeys();

				if( tempRs.next()) {
					result = tempRs.getInt( 1 );
					hostIds.put( domain, result );
				}else{
					LOG.error( "Unable to get the genrated node Id back" );
				}
			} catch( Exception e ) {
				LOG.error( "Exception while inserting new host:", e );
			}
		}

		LOG.debug( "Got id:" + result + " for host:" + domain );
		return result;
	}

	@Override
	public int getPathId( int hostId, String path ) {
		int result = 0;

		if( null == path ) {
			LOG.debug( "path is null" );
			path = "/";
		}

		if( "" == path ) {
			LOG.debug( "path is empty" );
			path = "/";
		}		

		LOG.debug( "hostId:" + hostId + " path:" + path );

		checkConnection();
		++counter;
		try {

			if( null == psUrl ) {
				psUrl = conn.prepareStatement( 
						"INSERT INTO urls ( host_id , path ) VALUES (?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
						, Statement.RETURN_GENERATED_KEYS
						);
			}

			psUrl.setInt( 1, hostId );
			psUrl.setString( 2, path );
			psUrl.executeUpdate();
			ResultSet tempRs = psUrl.getGeneratedKeys();

			if( tempRs.next()) {
				result = tempRs.getInt( 1 );
			}else{
				LOG.error( "Unable to get the genrated url Id back" );
			}

		} catch( Exception e ) {
			LOG.error( "Exception while inserting new host:", e );
		}

		LOG.debug( "Returning id:" + result + " for path:" + path );
		return result;
	}

	@Override
	public boolean addNode( int hostId, int pathId, int hash, String xpath ) {
		//		boolean result = false;
		//		long nodeId = 0;
		//		
		//		checkConnection();
		//		counter += 2;
		//		try {
		//
		//			if( null == psNode ) {
		//				psNode = conn.prepareStatement( 
		//						"INSERT INTO nodes ( host_id, hash, xpath ) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
		//						, Statement.RETURN_GENERATED_KEYS
		//						);
		//			}
		//
		//			
		//
		//			psNode.setInt( 1, hostId );
		//			psNode.setInt( 2, hash );
		//			psNode.setString( 3, xpath);
		//
		//			psNode.executeUpdate();
		//
		//			tempRs = psNode.getGeneratedKeys();
		//
		//			if( tempRs.next()) {
		//				nodeId = tempRs.getLong( 1 );
		//			}else{
		//				// TODO die here
		//				LOG.error( "Unable to get the node genrated Id back" );
		//			}
		//
		//			
		//			if( null == psFrequency ) {
		//				psFrequency = conn.prepareStatement( "INSERT INTO frequency( node_id,url_id ) VALUES (?, ?);" );
		//			}
		//			
		//			psFrequency.setLong( 1, nodeId );
		//			psFrequency.setInt( 2, pathId );			
		//			
		//			try {
		//				psFrequency.executeUpdate();
		//				result = true;
		//			} catch( java.sql.BatchUpdateException e ) {
		//			//catch( java.sql.SQLIntegrityConstraintViolationException e ) {
		//				LOG.debug( "The node "+nodeId + " alredy exist in page:" + pathId );
		//			}
		//			
		//		} catch (SQLException e) {
		//			// TODO check for existing node is part of normal operation and not an error
		//			// TODO die here
		//			LOG.error( "Exception while adding a new node:", e );
		//		}
		//		
		//		return result;

		boolean result = false;
		long nodeId = 0;


		try {
			//add hash-host-xapth in nodes table and extract id3 from it

			if( null == psNode ) {
				psNode = conn.prepareStatement( 
						"INSERT INTO nodes ( host_id, hash, xpath_id ) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
						, Statement.RETURN_GENERATED_KEYS
						);
			}

			psNode.setInt( 1, hostId );
			psNode.setInt( 2, hash );
			psNode.setInt( 3, xpath.hashCode() );

			psNode.executeUpdate();

			tempRs = psNode.getGeneratedKeys();

			if( tempRs.next()) {
				nodeId = tempRs.getLong( 1 );
			}else{
				// TODO die here
				LOG.error( "Unable to get the node genrated Id back" );
			}

		}catch (SQLException e) {
			// TODO check for existing node is part of normal operation and not an error
			// TODO die here
			LOG.error( "Exception while adding a new node:" , e );
		}


		//add id# and path in frequency table
		try {
			if( null == psFrequency ) {
				psFrequency = conn.prepareStatement( "INSERT INTO frequency( node_id,url_id ) VALUES (?, ?);" );
			}
			psFrequency.setLong( 1, nodeId );
			psFrequency.setInt( 2, pathId );			


			psFrequency.executeUpdate();
			result = true;
		} catch( java.sql.BatchUpdateException re ) {

			result = false;

		}catch(Exception e){
			LOG.error( "Error while adding a id in frequency table" );
		}

		
		
		//check the frequency of a xpath (how many path exist in same hash, xpath, host)
		if (result){

			try {	

				psGetFrequency=conn.prepareStatement( 
						"SELECT count(url_id ) FROM frequency  WHERE node_id=?  ;" 
						);
				psGetFrequency.setLong( 1, nodeId );

				tempRs = psGetFrequency.executeQuery();

				if( tempRs.next() ) {
					result = (tempRs.getInt( 1 )<5);
				}else{
					LOG.error( "Node " + xpath + " from host:" +hostId + " with hash code:"+ hash +" is not in database." );
				}

			} catch (SQLException e) {
				LOG.error("Exception while getting node frequency: " , e);
			}

		}


		return result;

	}

	@Override
	public int getNodeFreq( int hostId, int hash, String xpath ) {
		//		int result = 0;
		//
		//		checkConnection();
		//		++counter;
		//		try {	
		//
		//			psGetFrequency=conn.prepareStatement( 
		//					"SELECT count(url_id ) FROM nodes JOIN frequency ON ( node_id = id ) WHERE host_id=? AND hash=? AND xpath=? ;" 
		//					);
		//			psGetFrequency.setInt( 1, hostId );
		//			psGetFrequency.setInt( 2, hash );
		//			psGetFrequency.setString(3, xpath);
		//
		//			tempRs = psGetFrequency.executeQuery();
		//
		//			if( tempRs.next() ) {
		//				result = tempRs.getInt( 1 );
		//			}else{
		//				LOG.debug( "Node " + xpath + " from host:" +hostId + " with hash code:"+ hash +" is not in database." );
		//			}
		//
		//		} catch (SQLException e) {
		//			LOG.error("Exception while getting node frequency: ",e);
		//		}
		//
		//		return result;

		int result = 0;

		try {	
			if(null == psgetfreq){
				psgetfreq=conn.prepareStatement( 
						"SELECT count(url_id ) FROM nodes JOIN frequency ON ( node_id = id ) WHERE host_id=? AND hash=? AND xpath_id=? ;" 
						);
			}
			psgetfreq.setInt( 1, hostId );
			psgetfreq.setInt( 2, hash );
			psgetfreq.setInt(3, xpath.hashCode());

			tempRs = psgetfreq.executeQuery();

			if( tempRs.next() ) {
				result = tempRs.getInt( 1 );
			}else{
				System.out.println( "Node " + xpath + " from host:" +hostId + " with hash code:"+ hash +" is not in database." );
			}

		} catch (SQLException e) {
			System.out.println("Exception while getting node frequency: "+e);
		}

		return result;

	}


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

