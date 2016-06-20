package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.Set;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.HashMap;
  

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	static private int dbNumber;
	static private JedisPool pool;
	static private final JedisPoolConfig poolConfig = new JedisPoolConfig();
	
	static private final ScanParams scanParams = new ScanParams();

	public final Map< NodeId, NodeValue > read = new HashMap< NodeId, NodeValue >( 1024 );
	
	private Jedis jedis;
	private byte[] pathIdBytes = new byte[ Integer.BYTES ];
	
	@Override
	static synchronized public void setConf( Configuration conf ) {
		Storage.setConf( conf );
		
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

		//poolConfig = new JedisPoolConfig();
		
		scanParams.count( 1024 );

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
	
	
	public void saveNode( NodeId pid, Set<Integer> paths ) {
		int achived = 3;
		int size = paths.size();
		
		
		byte pathBytes[][] = new byte[size][ Integer.BYTES ];
		
		for( Integer id : paths ) {
			ByteBuffer.wrap( pathBytes[ --size ] ).putInt( id );
		}
		
		while( 0 != achived ) {
			try{
				--achived;
				jedis.sadd( ( new HostCache( hostId, pid ) ).getBytes(), pathBytes );
				
				break;
			}catch(Exception e){

				LOG.error("Exception while saving node:" + pid + " attempt(s) left:" + achived, e );				
				initConnection();
				
			}
		}
		
		if( 0 == achived ) {
			LOG.error( "Could not save node:" + pid + ", skiping it." );
		}
		
	}
	
	
	public LRUCache< String, NodeId > loadHostInfo( Integer hid, LRUCache< String, NodeId > hostCache ) {
		Jedis jedis = getConnection();
		
		String hostKey = NodeUtil.encoder.encodeToString(  ByteBuffer.allocate( Integer.BYTES ).putInt( hid ).array() ) + ":H";
		boolean loop = true;
		String cursor = "0";
		
		while( loop ) {
			ScanResult<String> scanResult = jedis.sscan( hostKey, cursor, scanParams );
			cursor = scanResult.getStringCursor();
			
			List<String> list = scanResult.getResult();
			List< Response< Long > > sizes = new ListArray< Response< Long > >( list.size() );
			
			if( null != list ) {
				Pipeline p = jedis.pipelined();
				
				for( String nodeKey : list ) {
					p.scard( nodeKey );
				}
			}
			
			
			
		}
		
		
		return hostCache;
	}
	
	@Override
	public void incNodeFreq( NodeId pid, NodeValue val ) {

		boolean achived = false;
		int times = counter;

		while( ! achived ) {
			try{

				++counter;
				
				HostCache id = new HostCache( hostId, pid );
				byte nodeBytes[] = id.getBytes();
				jedis.sadd( nodeBytes, pathIdBytes );
				
				// jedis.sadd
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
	protected void addToBackendList( NodeId id ) {

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


	@Override
	protected Map<NodeId, NodeValue> getBackendFreq() {
		for( NodeId temp : missing ) {
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
