package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.Set;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;  

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.Harvester;
import com.doslocos.nutch.harvester.HostCache;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.NodeValue;
import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

//import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class Redis extends Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Redis.class );
	static public final String CONF_PREFIX = Harvester.CONF_PREFIX + "redis.";
	static public int dbNumber, bucketSize;

	static private JedisPool pool;
	static private final JedisPoolConfig poolConfig = new JedisPoolConfig();
	static private final ScanParams scanParams = new ScanParams();

	
	private Jedis jedis;
	private byte[] pathIdBytes = new byte[ Integer.BYTES ];
	
	@Override
	static public synchronized void setConf( Configuration conf ) {
		Storage.setConf( conf );
		
		LOG.debug( "Initilizing Redis storage." );
		
		String redisHost = conf.get( CONF_PREFIX + "host", "localhost" );
		int redisPort = conf.getInt( CONF_PREFIX + "port", 6379 );
		int redisDb = conf.getInt( CONF_PREFIX + "db", 15 );
		int redisTimeOut = conf.getInt( CONF_PREFIX + "dbTimeOut", 0 );
		LOG.debug( "host:" + redisHost + ":" + redisPort +" db:"+ redisDb + " timeout:" + redisTimeOut );
		
		boolean setTestOnBorrow = conf.getBoolean( CONF_PREFIX + "setTestOnBorrow", true );
		boolean setTestOnReturn = conf.getBoolean( CONF_PREFIX + "setTestOnReturn", true );
		boolean setTestWhileIdle = conf.getBoolean( CONF_PREFIX + "setTestWhileIdle", true );
		int setMaxTotal = conf.getInt( CONF_PREFIX + "setMaxTotal", 32 );
		int setMaxIdle = conf.getInt( CONF_PREFIX + "setMaxIdle", 8 );
		LOG.debug( "Pool: onBorrow:" + setTestOnBorrow + "  onReturn:" + setTestOnReturn + " whileIdle:" + setTestWhileIdle );
		LOG.debug( "Pool: MaxTotal:" + setMaxTotal + " MaxIdle:" + setMaxIdle );

		//poolConfig = new JedisPoolConfig();
		bucketSize = conf.getInt( CONF_PREFIX + "bucket_size", 1024 );
		scanParams.count( bucketSize );

		poolConfig.setTestOnBorrow( setTestOnBorrow );
		poolConfig.setTestOnReturn( setTestOnReturn );
		poolConfig.setTestWhileIdle( setTestWhileIdle);
		poolConfig.setMaxTotal( setMaxTotal ); 
		poolConfig.setMaxIdle( setMaxIdle );

		pool = new JedisPool( poolConfig, redisHost, redisPort, redisTimeOut );	

	}

	static public Jedis getConnection() {
		Jedis conn = null;
		
		try{			
			conn = pool.getResource(); 
			conn.select( dbNumber );
				
			LOG.debug( "Getting a new connection." );
		}catch( Exception e ){
			LOG.error( "while getting the connection:", e );
		}
		
		return conn;
	}
	
	
 	public Redis( String host, String path ) {
		super( host, path );
		ByteBuffer.wrap( pathIdBytes ).putInt( pathId );
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
		Jedis jedis = getConnection();		
				
		boolean loop = true;
		String cursor = "0";
		
		while( loop ) {
			ScanResult<String> scanResult = jedis.sscan( hostCache.getKey(), cursor, scanParams );
			cursor = scanResult.getStringCursor();
			
			List<String> list = scanResult.getResult();
			LinkedHashMap< String, Response< Long > > nodes = new LinkedHashMap< String, Response< Long > >( list.size() );
			
			Pipeline p = jedis.pipelined();
				
			for( String nodeKey : list ) {
				nodes.put( nodeKey, p.scard( nodeKey + ":" + hostCache.getKey() ) );
			}
				
			p.sync();
			
			for( Map.Entry<String, Response<Long>> e : nodes.entrySet() ) {
				NodeId nodeId = new NodeId( e.getKey() );
				nodeId.numSavedPath = e.getValue().get();
				hostCache.nodes.put( e.getKey(), nodeId);
			}
			
			if( "0" == cursor ) loop = false;
		}

		return hostCache;
	}
	
	
	public void saveHostInfo( HostCache hostCache ) { 
		Pipeline p = getConnection().pipelined();
		
		synchronized( hostCache ) {
				
			// create host key	
			p.sadd( hostCache.getKey(), hostCache.nodes.keySet().toArray( new String[0] ) ); 
			p.sync();
			
			int numOfWrite = 0;
			for( Map.Entry< String, NodeId > entry : hostCache.nodes.entrySet() ) {
				int thisPathsSize = entry.getValue().paths.size(); 
				
				if( thisPathsSize > Harvester.ft_write ) {
					p.sadd( entry.getKey() + ":" + hostCache.getKey(), entry.getValue().paths.toArray( new String[0] ) );
				
					numOfWrite += thisPathsSize;
					
					entry.getValue().numSavedPath += thisPathsSize;
					entry.getValue().paths.clear();				 
				}
				
				if( numOfWrite > bucketSize ) {
					numOfWrite -= bucketSize;
					p.sync();			
				}
			}
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
