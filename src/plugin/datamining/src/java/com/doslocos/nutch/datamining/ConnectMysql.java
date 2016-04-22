package com.doslocos.nutch.datamining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectMysql extends Knowledge {

	private static String SCHEMA;
	private static String USER;
	private static String PASS;
	private static String DBHOST;
	private static int freq_tr;
	
	private static BasicDataSource poolDS;
	public Connection conn = null;
	
	public static PreparedStatement  psNode, psFrequency, psGetFrequency, psgetfreq;
	public static ResultSet tempRs = null;

	private static int[] executeResult = null;

	public static final Logger LOG = LoggerFactory.getLogger( ConnectMysql.class );


	public ConnectMysql( Configuration conf ) {
		DBHOST = conf.get("doslocos.training.database.host");
		SCHEMA = conf.get("doslocos.training.database.schema");
		USER = conf.get("doslocos.training.database.username");
		PASS = conf.get("doslocos.training.database.password");
		freq_tr = conf.getInt( "doslocos.training.frequency_threshould" , 2  )+1;

		LOG.debug("Connection class called");


		poolDS = new BasicDataSource();

		poolDS.setDriverClassName("org.mariadb.jdbc.Driver");

		poolDS.setUrl("jdbc:mysql://"+DBHOST+"/"+SCHEMA);

		poolDS.setUsername(USER);

		poolDS.setPassword(PASS);


		initConnection( false );
	}

	private void checkConnection() {
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
			LOG.error( "Unable to renew the database connection."  );
		}


	}

	private boolean initConnection( boolean force ) {
		if( force || null == conn ) {
			try {

				//				LOG.debug("Connection to database stablished");
				//				Class.forName("org.mariadb.jdbc.Driver");
				//				String sqlConnection="jdbc:mysql://" + DBHOST + "/" + SCHEMA;
				//				conn = DriverManager.getConnection(sqlConnection, USER, PASS );

				conn = poolDS.getConnection();

				LOG.debug("connection to pool established");
			} catch( Exception e ) {
				LOG.error( "Failed to establish connection:", e );
				return false;
			}
		}

		return true;
	}


	@Override
	public int getHostId( String domain ) {

		Integer result = domain.hashCode();


		return result;
	}

	@Override
	public int getPathId( int hostId, String path ) {


		if ( null == path ){
			path = "/" ;
		}

		int result = path.hashCode();

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

		boolean result = true;

		checkConnection();

		try {


			if( null == psNode ) {
				psNode = conn.prepareStatement( 
						"INSERT INTO nodes ( host_id, hash, xpath_id ) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
						, Statement.RETURN_GENERATED_KEYS
						);
			}

			psNode.setInt( 1, hostId );
			psNode.setInt( 2, hash );
			psNode.setInt( 3, xpath.hashCode() );

			psNode.addBatch();

		}catch(Exception e){
			LOG.error( "Error while adding a id in frequency table" , e  );
		}
		//
		//			//	psNode.executeUpdate();
		//
		//			tempRs = psNode.getGeneratedKeys();
		//
		//			if( tempRs.next()) {
		//				nodeId = tempRs.getLong( 1 );
		//
		//			}else{
		//
		//				LOG.debug( "Unable to get the node genrated Id back" );
		//			}
		//
		//		}catch (SQLException e) {
		//
		//			LOG.error( "Exception while adding a new node:" , e );
		//		}
		//
		//
		//		//add id# and path in frequency table
		//		try {
		//			if( null == psFrequency ) {
		//				psFrequency = conn.prepareStatement( "INSERT INTO frequency( node_id,url_id ) VALUES (?, ?);" );
		//			}
		//			psFrequency.setLong( 1, nodeId );
		//			psFrequency.setInt( 2, pathId );			
		//
		//
		//			psFrequency.executeUpdate();
		//			result = true;
		//
		//		} catch( java.sql.BatchUpdateException re ) {
		//
		//			result = false;
		//
		//		}catch(Exception e){
		//			LOG.error( "Error while adding a id in frequency table" , e  );
		//		}
		//


		//check the frequency of a xpath (how many path exist in same hash, xpath, host)


		//		try {	
		//
		//			psGetFrequency=conn.prepareStatement( 
		//					"SELECT count(url_id ) FROM frequency  WHERE node_id=?  ;" 
		//					);
		//			psGetFrequency.setLong( 1, nodeId );
		//
		//			tempRs = psGetFrequency.executeQuery();
		//			counter ++;
		//
		//			if( tempRs.next() ) {
		result = (getNodeFreq(hostId, hash, xpath ) < freq_tr );
		//			}else{
		//				LOG.debug( "Node " + xpath + " from host:" + hostId + " with hash code:"+ hash +" is not in database." );
		//			}

		//		} catch (SQLException e) {
		//			LOG.error("Exception while getting node frequency: " , e);
		//		}




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

		checkConnection();

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
				LOG.debug( "Node " + xpath + " from host:" +hostId + " with hash code:"+ hash +" is not in database." );
			}

		} catch (SQLException e) {
			LOG.error("Exception while getting node frequency in adding a node: " , e);
		}
		counter++;
		return result;

	}

	public boolean emptyBatch(int pathId){

		boolean result = false ;
		try {
			executeResult = psNode.executeBatch();

			tempRs = psNode.getGeneratedKeys();

			for (int j =0 ;j< executeResult.length;j++){
				tempRs.next();


				if( null == psFrequency ) {
					psFrequency = conn.prepareStatement( "INSERT INTO frequency( node_id,url_id ) VALUES (?, ?);" );
				}
				psFrequency.setLong( 1, tempRs.getLong(1));
				psFrequency.setInt( 2, pathId );			

				psFrequency.addBatch();

			}
			psFrequency.executeBatch();
			counter += 2;
			LOG.info("finish to update batches");
			result = true;
		}catch(Exception e){
			LOG.error( "Error while adding a id in frequency table" , e  );
		}
		

		return result;
	}

	protected void finalize(){
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("Error happen while closing connection by finalize function" , e);
			}
			conn = null;
		}
	}

	//	public static void main(String[] args) {
	//		System.out.println("hi");
	//		
	//		
	//	}

}

