package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;  

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.HostCache;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.Settings;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;

//import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class Redis extends Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Redis.class );

	static private JedisPool pool;

	private Jedis jedis;
	private byte[] pathIdBytes = new byte[ Integer.BYTES ];
	
	
	static public synchronized void init() {
		Storage.init();
		
		if( null == pool ) {
			LOG.info( "Initilizing Redis storage." );
				
			pool = new JedisPool( Settings.Storage.Redis.poolConfig, Settings.Storage.Redis.host, Settings.Storage.Redis.port, Settings.Storage.Redis.timeOut );	
		}
	}

	static public Jedis getConnection() {
		Jedis conn = null;
		
		try{			
			LOG.info( "Getting a new connection." );
			
			conn = pool.getResource(); 
			conn.select( Settings.Storage.Redis.db );
			
			LOG.info( " returning the connection." );
		}catch( Exception e ){
			LOG.error( "while getting the connection:", e );
		}
		
		return conn;
	}
	
	
 	public Redis( String host, String path ) {
		super( host, path );
		ByteBuffer.wrap( pathIdBytes ).putInt( pathHash );
		initConnection();
	}

	
	private boolean initConnection() {
		boolean result = false ;

		if( null == jedis || ! jedis.isConnected() ) {
			jedis = getConnection();
		}

		result = true ;
		
		return result;
	}
	

	@Override
	public HostCache loadHostInfo( HostCache hostCache ) {
		Jedis conn = getConnection();
		LOG.info( "Loading host:" + hostCache );

		boolean loop = true;
		String cursor = "0";
		
		while( loop ) {
			ScanResult<String> scanResult = conn.sscan( hostCache.getKey(), cursor, Settings.Storage.Redis.scanParams );
			cursor = scanResult.getStringCursor();
			List<String> list = scanResult.getResult();
			
			LOG.info( "cursor is " + cursor + " result has:" + list.size() );
			
			LinkedHashMap< String, Response< Long > > nodes = new LinkedHashMap< String, Response< Long > >( list.size() );
			
			Pipeline p = conn.pipelined();
				
			for( String nodeKey : list ) {
				nodes.put( nodeKey, p.scard( nodeKey + ":" + hostCache.getKey() ) );
			}
			
			LOG.info( "about to sync, number of nodes is:" + nodes.size() );
			p.sync();
			
			for( Map.Entry<String, Response<Long>> e : nodes.entrySet() ) {
				NodeId nodeId = new NodeId( e.getKey() );
				nodeId.numSavedPath = new Long( e.getValue().get() ).intValue();
				hostCache.nodes.put( e.getKey(), nodeId);
			}
			
			if( cursor.equals( "0" ) ) loop = false;
		}

		return hostCache;
	}
	
	
	@Override
	public void saveHostInfo( HostCache hostCache ) { 
		Pipeline p = getConnection().pipelined();
		
		synchronized( hostCache ) {
				
			// create host key	
			p.sadd( hostCache.getKey(), hostCache.nodes.keySet().toArray( new String[0] ) ); 
			p.sync();
			
			int numOfWrite = 0;
			for( Map.Entry< String, NodeId > entry : hostCache.nodes.entrySet() ) {
				int thisPathsSize = entry.getValue().paths.size(); 
				
				if( thisPathsSize > Settings.Frequency.write ) {
					p.sadd( entry.getKey() + ":" + hostCache.getKey(), entry.getValue().paths.toArray( new String[0] ) );
				
					numOfWrite += thisPathsSize;
					
					entry.getValue().numSavedPath += thisPathsSize;
					entry.getValue().paths.clear();				 
				}
				
				if( numOfWrite > Settings.Storage.Redis.bucketSize ) {
					numOfWrite -= Settings.Storage.Redis.bucketSize;
					p.sync();			
				}
			}
			
			hostCache.needSave = false;
		}
		
		p.sync();
	}





	@Override
	protected void finalize(){
		System.err.println( "Redis finalize was called." );
		LOG.info( "Redis finalize was called." );
		if( null != jedis ) {
			try {
				jedis.close();
			} catch ( Exception e ) {
				LOG.error( "Exception whild closing the jedis connection.", e );
			}
			jedis = null;
		}

		super.finalize();
	}
	

	@Override
	public void pageEnd( boolean learn ) {
		LOG.debug( "PageEnd was called" );
		super.pageEnd(learn);
	}



}
