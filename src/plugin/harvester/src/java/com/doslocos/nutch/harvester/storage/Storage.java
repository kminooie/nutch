package com.doslocos.nutch.harvester.storage;


import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.doslocos.nutch.util.BytesWrapper;
import com.doslocos.nutch.util.NodeUtil;
import com.doslocos.nutch.harvester.Harvester;
import com.doslocos.nutch.harvester.HostCache;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.Settings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );

	static protected final AtomicInteger pageLearnedCounter = new AtomicInteger();
	
	static public ConcurrentHashMap< Integer, HostCache > mainCache;
	
	// static private int cacheHit = 0, cacheMissed = 0;
	
	
	public final Integer hostHash, pathHash;
	public final ByteBuffer pathKey;
	public final HostCache hostCache; 

	

	static public void dumpMainCache() {
		for( Map.Entry< Integer, HostCache > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			HostCache hc = entry.getValue();
			
			LOG.info( "hostId: " + hId );
			for( Iterator< Map.Entry< ByteBuffer, NodeId > > itr = hc.nodes.entrySet().iterator(); itr.hasNext(); ) {
				Map.Entry< ByteBuffer, NodeId > pageEntry = itr.next();
				Harvester.LOG.info( "node: " + pageEntry.getKey() + " size is:" + pageEntry.getValue().paths.size() );
			}
		}
	}
	

	static public void init() {

		// prevent being set more than once
		if( null == mainCache ) synchronized ( Storage.class ) {
			if( null == mainCache ) mainCache = new ConcurrentHashMap< Integer, HostCache >( Settings.Cache.hosts_per_job );
		} else {
			LOG.warn( "Seems like Init was called more than once." );
		}
		
		
//		boolean cleanUpEnalbed = conf.getBoolean( "doslocos.harvester.storage.cleanup", false );
//		int rangeMin = conf.getInt( "doslocos.harvester.storage.cleanup.min", 1322 );
//		int rangeMax = conf.getInt( "doslocos.harvester.storage.cleanup.max", 2893 );
//		if( cleanUpEnalbed ) {
//			cleanUpInterval = rangeMin + (int)( Math.random() * Math.abs( (rangeMax - rangeMin) + 1) );
//			cleanupHostIds = new HashSet<Integer>( cleanUpInterval );
//			LOG.info( "clean up enabled. interval:" + cleanUpInterval );
//		} else {
//			LOG.info( "Storage cleanup disabled." );
//		}
		
		
	}

	
 	public Storage( String host, String path ) {
		hostHash = NodeUtil.stringToId( host );
		//hostKey = NodeUtil.intToB64BBuffer( hostHash );

		if ( null == path ){
			path = "/" ;
			LOG.debug("a path with null value change to / value");
		}

		pathHash = NodeUtil.stringToId( path );
		pathKey = NodeUtil.intToB64BBuffer( pathHash );
		
		hostCache = loadHost( hostHash );		
	}

 	public HostCache loadHost( final Integer hash ) {
		HostCache hc = mainCache.get( hash );
		if( null == hc ) /*synchronized( mainCache ) */ {
			// if( null == hostCache ) {
				LOG.info( "loading host from storage" );
			
				hc = loadHostInfo( new HostCache( hash ) );
				mainCache.put( hash, hc );
				
			// }
		}
		LOG.info( "got host: " + hc );
		return hc;
	}

 	public int addNodeToThisHost( String nodeXpath, Integer nodeHash ) {
 		return hostCache.addNode( nodeXpath, nodeHash, pathKey.array() );
 	}
 	
	public void filterEnd() {
		pageEnd( false );
	}
	
	public void learnEnd() {
		if ( 0 == pageLearnedCounter.incrementAndGet() % Settings.Frequency.gc ) {
			LOG.info( "after learning gc is triggered, counter:" + pageLearnedCounter.intValue() );
			pruneMainCache();				
		}		

		// run the test at the end of learning
		if( null != Settings.Storage.testHost ) {
			synchronized( Settings.Storage.class ) {
				if( null != Settings.Storage.testHost ) {
					Settings.Storage.testHost = null;
					testSaveAndLoad();
				}
			}
		}
		
		
		pageEnd( true );
	}
	

	public void testSaveAndLoad() {
		//  begin test
		
		LOG.info( "begin test");
		String tHostName = "redis.io";
		LOG.info( "test host:" + tHostName );
		Integer tHostHash = NodeUtil.stringToId( tHostName );
		LOG.info( "test host hash:" + tHostHash );
		ByteBuffer tHostKey = NodeUtil.intToB64BBuffer( tHostHash );
		
		byte[] tempIntegerBuff = new byte[ Integer.BYTES * 2 ];
		
		NodeUtil.decoder.decode( tHostKey.array(), tempIntegerBuff );
		int tHostHash2 = ByteBuffer.wrap( tempIntegerBuff ).getInt( );
		LOG.info( "test host hash2:" + tHostHash2 );
		IntBuffer ib = ByteBuffer.wrap( tempIntegerBuff ).asIntBuffer();
		LOG.info( "test host hash2:" + ib );
		

		
		
		HostCache h0 = new HostCache( tHostKey.array() );
		LOG.info( "h0:" + h0 );
				
		HostCache h1 = new HostCache( tHostKey );
		LOG.info( "h1:" + h1 );
		
		HostCache h2 = new HostCache( tHostHash );
		LOG.info( "h2:" + h2 );
		
		LOG.info( "h0 ?= h1 " + h0.equals( h1 ) );
		LOG.info( "h1 ?= h2 " + h1.equals( h2 ) );
		LOG.info( "h2 ?= h1 " + h2.equals( h1 ) );
		LOG.info( "h1 hash:" + h1.hashCode() );
		LOG.info( "h2 hash:" + h2.hashCode() );
		
		LOG.info( "Begin Test" );
		
		saveHostInfo( h0 );
		LOG.info( "before test1, hostHash:" + tHostHash );
		HostCache test1 = loadHostInfo( new HostCache( tHostHash ) );
		LOG.info( "before test2, hostKey:" + tHostKey + "with value:" + new String( tHostKey.array() ) );
		HostCache test2 = loadHostInfo( new HostCache( h0.getB64Key( false ).array() ) );
		HostCache test3 = loadHostInfo( new HostCache( h0.getB64Key( false ) ) );
		
		LOG.info( "original:" + h0 );
		LOG.info( "test1:" + test1 );
		LOG.info( "test2:" + test2 );
		LOG.info( "test3:" + test3 );
		LOG.info( "original ?= 1 :" + h0.equals( test1 ) );
		LOG.info( "original ?= 2 :" + h0.equals( test2 ) );
		LOG.info( "original ?= 3 :" + h0.equals( test3 ) );
		LOG.info( "1 ?= 2 :" + test1.equals( test2 ) );
		LOG.info( "2 ?= 3 :" + test2.equals( test3 ) );
		// end test
	}


//	protected void finalize() {
//		System.err.println( "Storage finalize was called" );
//		LOG.info( "Storage finalize was called." );
//	}
	

	protected void pruneMainCache() {
		for( Map.Entry< Integer, HostCache > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			HostCache hc = entry.getValue();
			
			if( hc.needSave ) {
				if( hc.needPrune ) {
					LOG.info( "pruning hostId: " + hId + " with size " + hc.nodes.size() );
					hc.pruneNodes();
					LOG.info( "size after pruning: " + hc.nodes.size() );					
				}
				
				saveHostInfo( hc );
			}
		}
	}


	
	abstract public void pageEnd( boolean learn );
	
	// can't be static because abstract, should lock over hostCache in question
	abstract public HostCache loadHostInfo( HostCache hc );
	abstract public void saveHostInfo( HostCache hc );

}
