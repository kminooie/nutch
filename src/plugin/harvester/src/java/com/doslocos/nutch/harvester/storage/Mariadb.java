package com.doslocos.nutch.harvester.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;


public class Mariadb extends Storage {

	static private BasicDataSource poolDS;

	static public final Logger LOG = LoggerFactory.getLogger( Mariadb.class );

	private Connection conn = null;
	private PreparedStatement  psNode, psFrequency;
	private ResultSet tempRs = null;
	
	private int newNodes = 0;
	private int newUrls = 0;

	static public void set( Configuration conf ) {
		String DBHOST = conf.get("doslocos.harvester.mariadb.host", "localhost" );
		String SCHEMA = conf.get("doslocos.harvester.mariadb.schema", "nutch_harvester_db" );
		String USER = conf.get("doslocos.harvester.mariadb.username", "root" );
		String PASS = conf.get("doslocos.harvester.mariadb.password", "" );


		LOG.info( "storage url: jdbc:mysql://"+DBHOST+"/"+SCHEMA+" with user: " + USER );

		poolDS = new BasicDataSource();
		poolDS.setDriverClassName( "org.mariadb.jdbc.Driver" );
		poolDS.setUrl( "jdbc:mysql://"+DBHOST+"/"+SCHEMA );
		poolDS.setUsername( USER );
		poolDS.setPassword( PASS );

		try {
			Connection conn = poolDS.getConnection();

			if( null != conn ) {
				LOG.info( "MariaDB connected." );
			} else {
				LOG.error( "Failed to connect to MariaDB." );
				System.exit( 1 );
			}
		} catch( Exception e ) {
			LOG.error( "Exception initilizing the connection pool: ", e );
		}

		Storage.set( conf );
	}

	public Mariadb( String host, String path ) {
		super( host, path );
		checkConnection();

		try {
			// psGetFreq = conn.prepareStatement( 
			// 		"SELECT count( url_id ) FROM nodes JOIN frequency ON ( node_id = id ) WHERE host_id=? AND hash=? AND xpath_id=? ;" 
			// 		);

			psNode = conn.prepareStatement( 
					"INSERT INTO nodes ( host_id, hash, xpath_id ) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
					, Statement.RETURN_GENERATED_KEYS
					);

			psFrequency = conn.prepareStatement( "INSERT INTO frequency( node_id, url_id ) VALUES (?, ?);"
					);

			// psPageNodes = conn.prepareStatement( "SELECT xpath, hash, COUNT( url_id ) fq FROM ( SELECT f.node_id id , n.host_id host_id, n.xpath xpath, n.hash hash FROM nodes n, frequency f WHERE n.id = f.node_id and n.host_id = ? and f.url_id = ?) t  JOIN  frequency f2 ON ( f2.node_id = t.id ) GROUP BY f2.node_id ;"
			// 		);

		} catch( Exception e ) {
			LOG.error( "while preparing statements: ", e );
		}

		LOG.debug("Connection class created");
	}

	private void checkConnection() {
		boolean renew = false;
		try {
			if ( null == conn || conn.isClosed() ) renew = true;
		} catch ( Exception e ) {
			LOG.error( "Exception while trying to check connection:", e );
			renew = true;
		}

		if( renew ) {
			try {
				conn = poolDS.getConnection();
				LOG.debug( "got connection from pool" );
				renew = true;
			} catch( Exception e ) {
				LOG.error( "Failed to get connection from pool:", e );
			}
		}

		// if( renew ) {
		// 	LOG.error( "Unable to renew the database connection."  );
		// }


	}


	@Override
	public void addToBackendList( PageNodeId id ) {

		// checkConnection();

		try {			
			psNode.setInt( 1, hostId );
			psNode.setInt( 2, id.hash );
			psNode.setInt( 3, id.xpathId );
			
			psNode.addBatch();

			++newNodes;

		} catch(Exception e) {
			LOG.error( "Error while adding a id in frequency table" , e  );
		}

		if( 0 == newNodes % batchSize ) pageEnd();
	}


	@Override
	public boolean pageEnd(){

		boolean result = false ;
		checkConnection();
		try {
			int[] executeResult = psNode.executeBatch();

			tempRs = psNode.getGeneratedKeys();

			for (int j = 0 ; j < executeResult.length; ++j ) {
				tempRs.next();

				psFrequency.setLong( 1, tempRs.getLong(1));
				psFrequency.setInt( 2, pathId );			

				psFrequency.addBatch();

			}
			psFrequency.executeBatch();
			++counter;
			LOG.info("finish to update batches");
			result = true;
		}catch(Exception e){
			LOG.error( "Error while adding a id in frequency table" , e  );
		}		

		return result;
	}
	

	
	@Override
	protected Map<PageNodeId, NodeValue> getBackendFreq() {
	
		String sql = "SELECT t.id id, xpath, hash, count(url_id ) fq FROM (" 
				+ "SELECT f.node_id id , n.host_id host_id, n.xpath xpath, n.hash hash FROM nodes n, frequency f "
				+ "WHERE n.id = f.node_id AND n.host_id ="+ hostId+" AND f.url_id = "+pathId+" AND NOT ( ";
		
		String nodeList = "";
		for( PageNodeId temp : exclusion ) {			 
			if( 0 < nodeList.length()  ) nodeList += " OR ";
			nodeList += "( xpath ="+ temp.xpathId + "' AND hash =" + temp.hash + ")";
		}
		
		sql = sql + nodeList + ") )t  JOIN  frequency f2 ON ( f2.node_id = t.id ) GROUP BY f2.node_id ";
		
		HashMap< PageNodeId, NodeValue > readFreq = new HashMap< PageNodeId, NodeValue >();
		
		checkConnection();
		
		try {
			Statement stmt = conn.createStatement();
	        ResultSet rs = stmt.executeQuery( sql );

	        while( rs.next() ) {
	        	int nid = rs.getInt( "id" );
	        	PageNodeId pid = new PageNodeId( rs.getInt( "xpath" ), rs.getInt( "hash" ) );
	        	int fq = rs.getInt( "fq" );
	        	
	        	NodeValue val = new NodeValue( fq, nid);
	        	
	        	readFreq.put( pid, val );
	        }
		} catch( Exception e ) {
			LOG.debug( "Got Exception:", e );
		}
		
		return readFreq;
	}


	@Override
	public void incNodeFreq( PageNodeId id, NodeValue val ) {
	
		if( null == val ) {
			try {
				
				psNode.setInt( 1, hostId );
				psNode.setInt( 2, id.hash );
				psNode.setInt( 3, id.xpathId );
				
				psNode.addBatch();
	
				++newNodes;
	
			} catch(Exception e) {
				LOG.error( "Error while attempting to add a node" , e );
			}
		} else {
			try {
				psFrequency.setInt( 1, val.dbId );
				psFrequency.setInt( 2, pathId );
				
				psFrequency.addBatch();
				
				++newUrls;
				
				// size of psFrequency updates are much smaller
				if( 0 == newUrls % ( 3 * batchSize ) ) {
					psFrequency.executeBatch();
					++counter;
				}
				
			} catch( Exception e ) {
				LOG.error( "Error while adding a id in frequency table" , e );
			}
		}

		if( 0 == newNodes % batchSize ) pageEnd();
		
		
		
	}


	
	@Override
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

}

