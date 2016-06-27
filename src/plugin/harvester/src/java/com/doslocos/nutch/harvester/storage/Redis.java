package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.HostCache;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.Settings;
import com.doslocos.nutch.util.BytesWrapper;
import com.doslocos.nutch.util.NodeUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanResult;

//import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class Redis extends Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Redis.class );
	static public final byte[] INITIAL_CURSOR = new String( "0" ).getBytes();
	static public final byte[] SEPERATOR_bytes = Settings.Storage.SEPARATOR.getBytes();
	
	static private JedisPool pool;

	private Jedis jedis;
	
	
	static public void init() {
		Storage.init();
		
		if( null == pool ) synchronized( Redis.class ) {
			if( null == pool ) {
				// dumpPoolConfig( Settings.Storage.Redis.poolConfig );
				
				pool = new JedisPool( Settings.Storage.Redis.poolConfig, Settings.Storage.Redis.host, Settings.Storage.Redis.port, Settings.Storage.Redis.timeOut );
				LOG.info( "Instanciated redis pool." );
				
				// if( null == getConnection() ) {
				// 	LOG.error( "Got back null from getConnection" );
				// } else {
				// 	LOG.info( "Initilized Redis storage." );
				// }
			}
		} else {
			LOG.warn( "Init is called already" );
		}
	}

	static public Jedis getConnection() {
		Jedis conn = null;
		
		try{			
			// LOG.debug( "Getting a new connection." );
			// dumpPool();
			
			conn = pool.getResource(); 
			conn.select( Settings.Storage.Redis.db );
			
			// dumpPool();
			LOG.debug( "Got the connection, returning ..." );
		}catch( Exception e ){
			LOG.error( "while getting the connection:", e );
		}
		
		return conn;
	}
	
	static public void dumpPool() { 
		LOG.debug( "Pool settings:" );

		synchronized( pool ) {
			LOG.debug( "getNumActive:" + pool.getNumActive() );
			LOG.debug( "getNumIdle:" + pool.getNumIdle() );
			LOG.debug( "getNumWaiters:" + pool.getNumWaiters() );
			LOG.debug( "getMaxBorrowWaitTimeMillis:" + pool.getMaxBorrowWaitTimeMillis() );
			LOG.debug( "getMeanBorrowWaitTimeMillis:" + pool.getMeanBorrowWaitTimeMillis() );
			LOG.debug( "isClosed:" + pool.isClosed() );
		}
	}
	
	
	static public void dumpPoolConfig( JedisPoolConfig conf ) {
		LOG.debug( "PoolConfig Settings:" );

		synchronized( conf ) {
			LOG.debug( "getEvictionPolicyClassName:" + conf.getEvictionPolicyClassName() );
			LOG.debug( "getJmxNameBase:" + conf.getJmxNameBase() );
			LOG.debug( "getJmxNamePrefix:" + conf.getJmxNamePrefix() );
			LOG.debug( "getMaxIdle:" + conf.getMaxIdle() );
			LOG.debug( "getMaxTotal:" + conf.getMaxTotal() );
			LOG.debug( "getMaxWaitMillis:" + conf.getMaxWaitMillis() );
			LOG.debug( "getMinEvictableIdleTimeMillis:" + conf.getMinEvictableIdleTimeMillis() );
			LOG.debug( "getMinIdle:" + conf.getMinIdle() );
			LOG.debug( "getNumTestsPerEvictionRun:" + conf.getNumTestsPerEvictionRun() );
			LOG.debug( "getSoftMinEvictableIdleTimeMillis:" + conf.getSoftMinEvictableIdleTimeMillis() );
			LOG.debug( "getTimeBetweenEvictionRunsMillis:" + conf.getTimeBetweenEvictionRunsMillis() );
			LOG.debug( "getBlockWhenExhausted:" + conf.getBlockWhenExhausted() );
			LOG.debug( "getFairness:" + conf.getFairness() );
			LOG.debug( "getJmxEnabled:" + conf.getJmxEnabled() );
			LOG.debug( "getLifo:" + conf.getLifo() );
			LOG.debug( "getTestOnBorrow:" + conf.getTestOnBorrow() );
			LOG.debug( "getTestOnCreate:" + conf.getTestOnCreate() );
			LOG.debug( "getTestOnReturn:" + conf.getTestOnReturn() );
			LOG.debug( "getTestWhileIdle:" + conf.getTestWhileIdle() );
		}
	}
	
 	public Redis( String host, String path ) {
		super( host, path );
		initConnection();
	}

	
	private void initConnection() {
		if( null == jedis || ! jedis.isConnected() ) {
			jedis = getConnection();
		}
	}
	

	@Override
	public HostCache loadHostInfo( HostCache hc ) {
		initConnection();
	
		Pipeline p = jedis.pipelined();
		byte[] cursor = INITIAL_CURSOR;
		BytesWrapper hostPostFix = hc.getKey( false );
		LOG.debug( " SEPARATOR + HOST : " + hostPostFix );
		
		LinkedHashMap< BytesWrapper, Response< Long > > nodes = new LinkedHashMap< BytesWrapper, Response< Long > >( 
				Settings.Cache.getInitialCapacity( Settings.Storage.Redis.bucketSize + 1 ),
				Settings.Cache.load_factor
			);
		
		do {
			// get the list of nodes for the given host
			ScanResult<byte[]> scanResult = jedis.sscan( hc.getKey( false ).getBytes(), cursor, Settings.Storage.Redis.scanParams );

			nodes.clear();
			
			cursor = scanResult.getCursorAsBytes();
			List<byte[]> nodeKeyList = scanResult.getResult();
			
			if( LOG.isDebugEnabled() ) {
				LOG.debug( "cursor is " + new String( cursor ) + " result has:" + nodeKeyList.size() );
			}			
				
			for( byte[] nodeByteKey : nodeKeyList ) {
				
				BytesWrapper nodeKey = new BytesWrapper( nodeByteKey );
				
				// byte[] key = new byte[ nodeByteKey.length + hostPostFix.length ];
				// System.arraycopy( nodeByteKey, 0, key, 0, nodeByteKey.length);
				// System.arraycopy( hostPostFix, 0, key, nodeByteKey.length, hostPostFix.length);
				
				nodes.put( nodeKey, p.scard( nodeByteKey ) );
			}
			
			LOG.info( "about to sync, number of nodes is:" + nodes.size() );
			p.sync();
			
			for( Map.Entry<BytesWrapper, Response<Long>> e : nodes.entrySet() ) {
				NodeId nodeId = new NodeId( new Long( e.getValue().get() ).intValue(), e.getKey() );
				if( ! e.getKey().equals( nodeId.getKey() ) ) {
					LOG.error(" sanity failed." );
					LOG.error( e.getKey() + " is not equal with: " + nodeId.getKey() );
				}

				hc.nodes.put( e.getKey(), nodeId);
			}
			
		} while( ! Arrays.equals( cursor, INITIAL_CURSOR ) );	

		LOG.info( "Loaded: " + hc );

		return hc;
	}
	
	
	@Override
	public void saveHostInfo( HostCache hc ) { 
		
		if( null == hc ) {
			LOG.error( "saveHostInfo was passed a null pointer." );
			return;
		}
		
		Pipeline p = jedis.pipelined();
		BytesWrapper hostPostFix = hc.getKey( false );

		synchronized( hc ) {
			// TODO fix this	
			// create host key	
			// adding while iterating over nodes, moved into the loop
			//p.sadd( hostPostFix.getBytes(), hc.getNodesKeys() );

			p.sync();
			
			int writeCounter = 0;
			
			for( Map.Entry< BytesWrapper, NodeId > node : hc.nodes.entrySet() ) {
				int recentFrequency = node.getValue().getRecentFrequency(); 
				int oldFrequency = node.getValue().getFrequency() - recentFrequency;
				
				if( recentFrequency > Settings.Frequency.write ) {
					// String[] pathsKeys = node.getValue().getPathsKeysStrings();
					
					if( oldFrequency < Settings.Frequency.max ) {
						// ignoring host postfix // + hostPostFix
						p.sadd( hostPostFix.getBytes(), node.getKey().getBytes() );
						p.sadd( node.getKey().getBytes(), node.getValue().getPathsKeysByteArr() );						
						++writeCounter; // += recentFrequency;
					} else {
						LOG.warn( "throwing out paths of populor node:" + node.getKey() );
					}
					
					if( writeCounter > Settings.Storage.Redis.bucketSize ) {
						p.sync();
						writeCounter = 0;		
					}
				}			
			}
			
			if( writeCounter > 0 ) {
				p.sync();	
			}

			hc.needSave = false;
		}

	}


	@Override
	protected void finalize(){
		// System.err.println( "Redis finalize was called." );
		// LOG.info( "Redis finalize was called." );
		
		if( null != jedis ) {
			try {
				jedis.close();
			} catch ( Exception e ) {
				// LOG.error( "Exception whild closing the jedis connection.", e );
			}
			jedis = null;
		}

		// super.finalize();
	}
	

	@Override
	public void pageEnd( boolean learn ) {
		// LOG.debug( "PageEnd was called" );
	}

}
