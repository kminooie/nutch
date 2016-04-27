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
	boolean psNodeDirty = false, psFrequencyDirty = false;
	private ResultSet tempRs = null;

	private int newNodes = 0;
	private int newUrls = 0;

	static public void set( Configuration conf ) {
		String DBHOST = conf.get("doslocos.harvester.mariadb.host", "localhost" );
		String SCHEMA = conf.get("doslocos.harvester.mariadb.schema", "nutch_harvester_db" );
		String USER = conf.get("doslocos.harvester.mariadb.username", "root" );
		String PASS = conf.get("doslocos.harvester.mariadb.password", "" );


		LOG.debug( "storage url: jdbc:mysql://"+DBHOST+"/"+SCHEMA+" with user: " + USER );

		poolDS = new BasicDataSource();
	 
		poolDS.setDriverClassName( "org.mariadb.jdbc.Driver" );
		poolDS.setUrl( "jdbc:mysql://"+DBHOST+"/"+SCHEMA+"?rewriteBatchedStatements=true" );
		poolDS.setUsername( USER );
		poolDS.setPassword( PASS );

		int maxTotal = conf.getInt("doslocos.harvester.mariadb.setMaxTotal", 32);
		int maxIdle = conf.getInt("doslocos.harvester.mariadb.setMaxIdle", 32);
		
		poolDS.setMaxTotal(maxTotal);
		poolDS.setMaxIdle(maxIdle);
		
		try {
			Connection conn = poolDS.getConnection();

			if( null != conn ) {
				LOG.debug( "MariaDB connected." );
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
			psNode = conn.prepareStatement( 
					"INSERT INTO nodes ( host_id, hash, xpath_id ) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID( id );"
					, Statement.RETURN_GENERATED_KEYS
					);

			psFrequency = conn.prepareStatement( "INSERT IGNORE INTO urls( node_id, url_id ) VALUES (?, ?);"
					);
		} catch( Exception e ) {
			LOG.error( "while preparing statements: ", e );
		}

		LOG.debug("Connection class created");
	}

	@Override
	public void addToBackendList( PageNodeId id ) {


		try {			
			psNode.setInt( 1, hostId );
			psNode.setInt( 2, id.hash );
			psNode.setInt( 3, id.xpathId );

			psNode.addBatch();

			++newNodes;

		} catch(Exception e) {
			LOG.error( "Error while adding a id in frequency table" , e  );
		}

		if( 0 == newNodes % batchSize ) updateDB();
	}



	@Override
	public void pageEnd(){

		updateDB();

		try {
			conn.commit();
			conn.close();
			LOG.debug( "Page Ended. counter is:" + counter );
		}catch(Exception e){
			LOG.error( "Error while adding a id in frequency table" , e  );
		}		

		super.pageEnd();
	}


	@Override
	public void incNodeFreq( PageNodeId id, NodeValue val ) {
	//	LOG.debug( "incNodeFreq: id:" + id + " val:" + val );
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

				if( 0 == newUrls % batchSize ) {
					psFrequency.executeBatch();
					++counter;
				}

			} catch( Exception e ) {
				LOG.error( "Error while adding a id in frequency table" , e );
			}
		}

		if( 0 == newNodes % batchSize ) updateDB();

	}

	protected void updateDB( ) {
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
			++counter;
			LOG.debug("updated nodes.");
		}catch(Exception e){
			LOG.error( "Error while updateing nodes:" , e  );
		}

		try {
			psFrequency.executeBatch();
			++counter;
			LOG.debug("updated frequency.");
		} catch( Exception e ) {
			LOG.error( "Error while updating frequency:" , e  );
		}
	}


	@Override
	protected Map<PageNodeId, NodeValue> getBackendFreq() {

		HashMap< PageNodeId, NodeValue > readFreq = new HashMap< PageNodeId, NodeValue >( 1024 );

		if( missing.size() > 0 ) {

			String sqlPrefix = "SELECT f.node_id node_id, xpath_id, hash, fq"  
					+ " FROM frequency f JOIN urls u ON( f.node_id =  u.node_id AND u.url_id = " + pathId + " ) "
					+ "WHERE ( xpath_id, hash ) IN ("

				, sqlPostfix = ")"
				, nodeList = "";

			int i = 0;

			for( PageNodeId temp : missing ) {			 
				nodeList += ",("+ temp.xpathId + "," + temp.hash + ")";
				++i;

				if( 0 == i % batchSize ) {
					readDB( sqlPrefix + nodeList.substring( 1 ) + sqlPostfix, readFreq );
					nodeList = "";
				}
			}

			if( 0 < nodeList.length() )
				readDB( sqlPrefix + nodeList.substring( 1 ) + sqlPostfix, readFreq );
		}		

		return readFreq;
	}


	protected void readDB( String sql, Map<PageNodeId, NodeValue> map ) {

		checkConnection();

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery( sql );

			//int c = 0;
			while( rs.next() ) {
				//++c;
				int nid = rs.getInt( "node_id" );
				PageNodeId pid = new PageNodeId( rs.getInt( "xpath_id" ), rs.getInt( "hash" ) );
				int fq = rs.getInt( "fq" );

				NodeValue val = new NodeValue( fq, nid);

				map.put( pid, val );
			}
			//LOG.debug( "got " + c + " items from database.");
		} catch( Exception e ) {
			LOG.error( "Got Exception:", e );
		}
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
				conn.setAutoCommit( false );

				conn.setTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );

				LOG.debug( "got connection from pool" );
				renew = false;
			} catch( Exception e ) {
				LOG.error( "Failed to get connection from pool:", e );
			}
		}



	}


}

