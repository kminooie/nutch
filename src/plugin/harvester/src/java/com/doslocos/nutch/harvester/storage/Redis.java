package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class Redis extends Storage {


	static public final Logger LOG = LoggerFactory.getLogger( Redis.class );

	static private int dbNumber;
	static private JedisPool pool;
	static private JedisPoolConfig poolConfig;

	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< PageNodeId, Integer> read = new LinkedHashMap< PageNodeId, Integer>();
	public final LinkedHashMap< PageNodeId, Integer> write = new LinkedHashMap< PageNodeId, Integer>();
	
	private Jedis jedis;
	private byte[] pathIdBytes = new byte[ Integer.BYTES ];
	
	
	static public void set(Configuration conf){
		Storage.set( conf );
		
		LOG.info( "Initilizing Redis storage." );
		
		String redisHost = conf.get( "doslocos.harvester.redis.host", "localhost" );
		int redisPort = conf.getInt( "doslocos.harvester.redis.port", 6379 );
		int redisDb = conf.getInt( "doslocos.harvester.redis.db", 15 );
		int redisTimeOut = conf.getInt( "doslocos.harvester.redis.dbTimeOut", 0 );
		LOG.info( "host:" + redisHost + ":" + redisPort +" db:"+ redisDb + " timeout:" + redisTimeOut );
		
		boolean setTestOnBorrow = conf.getBoolean( "doslocos.harvester.redis.setTestOnBorrow", true );
		boolean setTestOnReturn = conf.getBoolean( "doslocos.harvester.redis.setTestOnReturn", true );
		boolean setTestWhileIdle = conf.getBoolean( "doslocos.harvester.redis.setTestWhileIdle", true );
		int setMaxTotal = conf.getInt( "doslocos.harvester.redis.setMaxTotal",128);
		int setMaxIdle = conf.getInt( "doslocos.harvester.redis.setMaxIdle",128);
		LOG.info( "Pool: onBorrow:" + setTestOnBorrow + "  onReturn:" + setTestOnReturn + " whileIdle:" + setTestWhileIdle );
		LOG.info( "Pool: MaxTotal:" + setMaxTotal + " MaxIdle:" + setMaxIdle );

		poolConfig = new JedisPoolConfig();

		poolConfig.setTestOnBorrow( setTestOnBorrow );
		poolConfig.setTestOnReturn( setTestOnReturn );
		poolConfig.setTestWhileIdle( setTestWhileIdle);
		poolConfig.setMaxTotal( setMaxTotal ); 
		poolConfig.setMaxIdle( setMaxIdle );  

		pool = new JedisPool( poolConfig, redisHost, redisPort, redisTimeOut );	

		// initConnection();
	}

	
	public Redis( String host, String path ) {
		super( host, path );
		ByteBuffer.wrap( pathIdBytes ).putInt( pathId );
	}

	
	private boolean initConnection() {
		boolean result = false ;

		try{			
			if( null == jedis || ! jedis.isConnected() ) {
				jedis = pool.getResource(); 
				jedis.select( dbNumber );
				
				LOG.debug( "Getting a new connection." );
			}

			result = true ;

		}catch( Exception e ){

			LOG.error( "there is an error during connect to database", e );
		}
		
		return result;

	}

	protected void addToBackendList( PageNodeId id ) { 
		// currentMap2.put( id, null );
		addNode( new NodeId( pathId, id ) );
	}

	
	protected void addToBackendList( NodeId id ) { 
		// currentMap2.put( id, null );
		addNode( id );
	}
	
	
//	protected Map<PageNodeId, NodeValue> getBackendFreq() {
//		return read;
//	}
	
	
	public boolean addNode( NodeId id ) {

		boolean result = false;
		boolean achived = false;

		int times = counter;

		initConnection();
		while( ! achived ) {
			try{

				++counter;
				
				byte nodeBytes[] = id.getBytes();
				int pathAdded = jedis.sadd( nodeBytes, pathIdBytes ).intValue();
				int freqPath = jedis.scard( nodeBytes ).intValue();

				LOG.info( "adding " + id + " with fq:" + freqPath + " for path:" + path );
				read.put( id.pageNodeId, freqPath );

				if( 1 == pathAdded ) {

//					if(freqPath < 2 ){
						result=true;
//
//					}
				}
				
				if( freqPath >= cacheThreshould ) {
					cache.put( id, new NodeValue(freqPath ));
				}

				achived = true;
			}catch(Exception e){

				LOG.error("error happen here for pageId :" + id +" path: "+ path + " result is " + result+"  ", e );


				initConnection();
				if( counter > times + 2 ) {
					LOG.error( "Can't resolve the issue by reinitilizing the connection" );
					achived = true;
				}
			}
		}
		return result;
	}
	
	

	// @Override
	public int getNodeFreq(int hostId, int hash, String xpath) {


		int freq = 0;

		String xpathHashCode = Integer.toString( xpath.hashCode() );

		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( hostId ) + "_" + xpathHashCode;


		try{
			freq = jedis.scard( nodeKey.getBytes() ).intValue();

		}catch(Exception e){

			LOG.debug("error ocure during get node frequency"+"  ", e );

		}
		counter++;

		return freq;
	}
	


	protected void finalize(){
		if( null != jedis ) {
			try {
				jedis.close();
			} catch ( Exception e ) {
				LOG.error( "Exception whild closing the jedis connection.", e );
			}
			jedis = null;
		}

	}
	


	@Override
	public boolean pageEnd() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	protected Map<PageNodeId, NodeValue> getBackendFreq() {
		// TODO Auto-generated method stub
		return null;
	}

}
