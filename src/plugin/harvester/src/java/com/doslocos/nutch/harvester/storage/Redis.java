package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.Set;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

//import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class Redis extends Storage {


	static public final Logger LOG = LoggerFactory.getLogger( Redis.class );

	static private int dbNumber;
	static private JedisPool pool;
	static private JedisPoolConfig poolConfig;

	public final Map< PageNodeId, NodeValue > read = new HashMap< PageNodeId, NodeValue >( 1024 );
	
	private Jedis jedis;
	private byte[] pathIdBytes = new byte[ Integer.BYTES ];
	
	
	static public void set(Configuration conf){
		Storage.set( conf );
		
		LOG.debug( "Initilizing Redis storage." );
		
		String redisHost = conf.get( "doslocos.harvester.redis.host", "localhost" );
		int redisPort = conf.getInt( "doslocos.harvester.redis.port", 6379 );
		int redisDb = conf.getInt( "doslocos.harvester.redis.db", 15 );
		int redisTimeOut = conf.getInt( "doslocos.harvester.redis.dbTimeOut", 0 );
		LOG.debug( "host:" + redisHost + ":" + redisPort +" db:"+ redisDb + " timeout:" + redisTimeOut );
		
		boolean setTestOnBorrow = conf.getBoolean( "doslocos.harvester.redis.setTestOnBorrow", true );
		boolean setTestOnReturn = conf.getBoolean( "doslocos.harvester.redis.setTestOnReturn", true );
		boolean setTestWhileIdle = conf.getBoolean( "doslocos.harvester.redis.setTestWhileIdle", true );
		int setMaxTotal = conf.getInt( "doslocos.harvester.redis.setMaxTotal",128);
		int setMaxIdle = conf.getInt( "doslocos.harvester.redis.setMaxIdle",128);
		LOG.debug( "Pool: onBorrow:" + setTestOnBorrow + "  onReturn:" + setTestOnReturn + " whileIdle:" + setTestWhileIdle );
		LOG.debug( "Pool: MaxTotal:" + setMaxTotal + " MaxIdle:" + setMaxIdle );

		poolConfig = new JedisPoolConfig();

		poolConfig.setTestOnBorrow( setTestOnBorrow );
		poolConfig.setTestOnReturn( setTestOnReturn );
		poolConfig.setTestWhileIdle( setTestWhileIdle);
		poolConfig.setMaxTotal( setMaxTotal ); 
		poolConfig.setMaxIdle( setMaxIdle );  

		pool = new JedisPool( poolConfig, redisHost, redisPort, redisTimeOut );	

	}

	
	public Redis( String host, String path ) {
		super( host, path );
		ByteBuffer.wrap( pathIdBytes ).putInt( pathId );
		initConnection();
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
	
	
	@Override
	public void incNodeFreq( PageNodeId pid, NodeValue val ) {

		boolean achived = false;
		int times = counter;

		while( ! achived ) {
			try{

				++counter;
				
				NodeId id = new NodeId( hostId, pid );
				byte nodeBytes[] = id.getBytes();
				jedis.sadd( nodeBytes, pathIdBytes ).intValue();
				achived = true;
			}catch(Exception e){

				LOG.error("error happen here for pageId :" + pid +" path: "+ path, e );
				
				initConnection();
				if( counter > times + 3 ) {
					LOG.error( "Can't resolve the issue by reinitilizing the connection" );
					achived = true;
				}
			}
		}

	}
	
	
	@Override
	protected void addToBackendList( PageNodeId id ) {

		boolean result = false;
		boolean achived = false;

		int times = counter;

		while( ! achived ) {
			try{

				++counter;
				
				byte nodeBytes[] = id.getBytes();
				int freqPath = jedis.scard( nodeBytes ).intValue();

				//LOG.debug( "adding " + id + " with fq:" + freqPath + " for path:" + path );
				read.put( id, new NodeValue( freqPath ) );

				achived = true;
			}catch(Exception e){

				LOG.error("error happen here for pageId :" + id +" path: "+ path + " result is " + result+"  ", e );


				initConnection();
				if( counter > times + 3 ) {
					LOG.error( "Can't resolve the issue by reinitilizing the connection" );
					achived = true;
				}
			}
		}
	}


	@Override
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
	public void pageEnd( boolean learn ) {
		LOG.debug( "PageEnd was called" );
	}


	@Override
	protected Map<PageNodeId, NodeValue> getBackendFreq() {
		for( PageNodeId temp : missing ) {
			addToBackendList( temp );
		}
		
		return read;
	}


	@Override
	protected boolean cleanUpDb(Set<Integer> hostIds) {
		// TODO Auto-generated method stub
		return false;
	}

}
