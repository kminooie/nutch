package com.doslocos.nutch.harvester.storage;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.HostCache2;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.Settings;


public class Mariadb extends Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Mariadb.class );

	static private BasicDataSource poolDS;

	private Connection conn = null;
	private PreparedStatement  psNode, psFrequency;
	private ResultSet tempRs = null;

	private int newNodes = 0;
	private int newUrls = 0;

	
	static public void init() {
		Storage.init();

		
		poolDS = new BasicDataSource();

		poolDS.setDriverClassName( "org.mariadb.jdbc.Driver" );
		poolDS.setUrl( "jdbc:mysql://" + Settings.Storage.Mariadb.DBHOST + "/" + Settings.Storage.Mariadb.SCHEMA + "?rewriteBatchedStatements=true" );
		poolDS.setUsername( Settings.Storage.Mariadb.USER );
		poolDS.setPassword( Settings.Storage.Mariadb.PASS );
		poolDS.setMaxTotal( Settings.Storage.Mariadb.poolMaxTotal );
		poolDS.setMaxIdle( Settings.Storage.Mariadb.poolMaxIdle );

		try {
			Connection conn = checkConnection( null );

			if( null != conn ) {
				LOG.info( "MariaDB connected." );
			} else {
				LOG.error( "Failed to connect to MariaDB." );
				System.exit( 1 );
			}
			
			int tLevel = conn.getTransactionIsolation();

			if( Connection.TRANSACTION_READ_UNCOMMITTED == tLevel ) {
				LOG.info( "Transaction level is READ_UNCOMMITTED" );
			} else if( Connection.TRANSACTION_READ_COMMITTED == tLevel ) {
				LOG.info( "Transaction level is READ_COMMITTED" );
			} else if( Connection.TRANSACTION_REPEATABLE_READ == tLevel ) {
				LOG.info( "Transaction level is REPEATABLE_READ" );
			} else if( Connection.TRANSACTION_SERIALIZABLE == tLevel ) {
				LOG.info( "Transaction level is SERIALIZABLE" );
			} else if( Connection.TRANSACTION_NONE == tLevel ) {
				LOG.info( "Transaction level is NONE" );
			} else {
				LOG.warn( "Transaction level is " + tLevel );
			}			
			
			conn.close();
		} catch( Exception e ) {
			LOG.error( "Exception initilizing the connection pool: ", e );
		}		
	}
	

	static private Connection checkConnection( Connection conn ) {
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
				conn.setTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
				// conn.commit();
				
				LOG.debug( "got connection from pool" );
				renew = false;
			} catch( Exception e ) {
				LOG.error( "Failed to get connection from pool:", e );
			}
		}
		
		return conn;
	}

	public Mariadb( String host, String path ) {
		super( host, path );
		conn = checkConnection( conn );

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


/*
	public void addToBackendList( NodeId id ) {


		try {			
			psNode.setInt( 1, hostHash );
			psNode.setInt( 2, id.hash );
			psNode.setInt( 3, id.xpathId );

			psNode.addBatch();

			++newNodes;

		} catch(Exception e) {
			LOG.error( "Error while adding an id in frequency table" , e  );
		}

		// if( 0 == newNodes % writeBSize ) updateDB();
	}
*/

	@Override
	public void pageEnd( boolean learn ){

		updateDB();
		
		

		try {
			conn.commit();
			
			if( null != psNode ) psNode.close();
			if( null != psFrequency ) psFrequency.close();
			
			conn.close();
		}catch(Exception e){
			LOG.error( "Exception in pageEnd" , e  );
		}

	}


/*
	
	public void incNodeFreq( NodeId id) {
		int writeBSize = 2; // added so I can compile
		//	LOG.debug( "incNodeFreq: id:" + id + " val:" + val );
		if( false ) {
			try {

				psNode.setInt( 1, hostHash );
				psNode.setInt( 2, id.hash );
				psNode.setInt( 3, id.xpathId );

				psNode.addBatch();

				++newNodes;

			} catch(Exception e) {
				LOG.error( "Error while attempting to add a node" , e );
			}
		} else {
			try {
				// psFrequency.setInt( 1, val.dbId );
				psFrequency.setInt( 2, pathHash );

				psFrequency.addBatch();

				++newUrls;

				if( 0 == newUrls % writeBSize ) {
					psFrequency.executeBatch();
					
				}

			} catch( Exception e ) {
				LOG.error( "Error while adding a id in frequency table" , e );
			}
		}

		if( 0 == newNodes % writeBSize ) updateDB();

	}
*/

	protected void updateDB( ) {
		conn = checkConnection( conn );
		try {
			int[] executeResult = psNode.executeBatch();

			tempRs = psNode.getGeneratedKeys();

			for (int j = 0 ; j < executeResult.length; ++j ) {
				tempRs.next();

				psFrequency.setLong( 1, tempRs.getLong(1));
				psFrequency.setInt( 2, pathHash );			

				psFrequency.addBatch();

			}

			//LOG.debug("updated nodes.");
		}catch(BatchUpdateException bue){

			LOG.debug("take care about batch update exception");
			//TODO solve the transaction exception and go and do it again

		}catch(Exception e){
			LOG.error( "Error while updateing nodes:" , e  );
		}

		try {
			psFrequency.executeBatch();

			//LOG.debug("updated frequency.");
		} catch( Exception e ) {
			LOG.error( "Error while updating frequency:" , e  );
		}
	}


/*
	protected Map<NodeId, NodeValue> getBackendFreq() {

		HashMap< NodeId, NodeValue > readFreq = new HashMap< NodeId, NodeValue >( 1024 );

		if( missing.size() > 0 ) {

			String sqlPrefix = "SELECT n.id  node_id, n.xpath_id xpath_id, n.hash hash, fq"  
					+ " FROM nodes n"
					+ " JOIN urls u ON( n.id =  u.node_id AND u.url_id = " + pathHash + " ) "
					+ " JOIN frequency f ON( n.id = f.node_id )"
					+ " WHERE n.host_id = " + hostHash + " AND ( n.xpath_id, n.hash ) IN ("

					, sqlPostfix = ")"
					, nodeList = ""
					;

			int i = 0;

			for( NodeId temp : missing ) {			 
				nodeList += ",("+ temp.xpathId + "," + temp.hash + ")";
				++i;

				if( 0 == i % readBSize ) {
					readDB( sqlPrefix + nodeList.substring( 1 ) + sqlPostfix, readFreq );
					nodeList = "";
				}
			}

			if( 0 < nodeList.length() )
				readDB( sqlPrefix + nodeList.substring( 1 ) + sqlPostfix, readFreq );
		}		

		return readFreq;
	}
*/

	protected void readDB( String sql  ) {

		conn = checkConnection( conn );

		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery( sql );

			//int c = 0;
			while( rs.next() ) {
				//++c;
//				int nid = rs.getInt( "node_id" );
//				NodeId pid = new NodeId( rs.getInt( "xpath_id" ), rs.getInt( "hash" ) );
//				int fq = rs.getInt( "fq" );

				// NodeValue val = new NodeValue( fq, nid);
				// map.put( pid, val );
			}
			//LOG.debug( "got " + c + " items from database.");
		} catch( Exception e ) {
			LOG.error( "Got Exception:", e );
		}
	}


	//the sql must be correct
	protected boolean cleanUpDb( Set<Integer> hostIds ){
		boolean result = true ;
		
		String sqlCommand = "DELETE n,u FROM nodes n"
				+ " JOIN urls u ON ( n.id = u.node_id )"
				+ " JOIN frequency f ON ( n.id = f.node_id )"
				+ " WHERE fq < 2";
		
		if( hostIds.size()  > 0 ) {
			String hostNameList = "";
			for (Integer hostId: hostIds){
				hostNameList += "," + hostId;
			}

			hostIds.clear();
			hostNameList = hostNameList.substring( 1 );
			
			sqlCommand += " AND n.host_id IN ( " + hostNameList + ")"; 
		}
		
		sqlCommand += ";";

		try{
			conn = checkConnection( conn );
			Statement stmtquerry = conn.createStatement();
			stmtquerry.executeQuery( sqlCommand );



		}catch(Exception e){
			result = false ;
			LOG.error("Exception while clean nodes which have frequency value 1",e);
		}

		return result;
	}


	@Override
	public HostCache2 loadHostInfo( HostCache2 hostCache ) {
		return new HostCache2( "nohost".getBytes() );
	}

	@Override
	public void saveHostInfo( HostCache2 hostCache ) {
		
	}
	
/*	
	@Override
	protected void finalize(){
		System.out.println( "mariadb finalize was called." );
		LOG.info( "mariadb finalize was called." );
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("Error happen while closing connection by finalize function" , e);
			}
			conn = null;
		}
		// super.finalize();
	}
*/

}

