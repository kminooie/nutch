package com.doslocos.nutch.harvester.storage;


import java.nio.ByteBuffer;
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
	
	// static public ConcurrentHashMap< Integer, HostCache > mainCache;
	static public ConcurrentHashMap< BytesWrapper, HostCache > mainCache;
	
	// static private int cacheHit = 0, cacheMissed = 0;
	
	
	public final Integer hostHash, pathHash;
	public final BytesWrapper hostKey, pathKey;

	public final HostCache hostCache; 

	

	static public void dumpMainCache() {
		for( Map.Entry< BytesWrapper, HostCache > entry: mainCache.entrySet() ) {
			BytesWrapper hId = entry.getKey();
			HostCache hc = entry.getValue();
			
			LOG.info( "hostId: " + hId );
			for( Iterator< Map.Entry< BytesWrapper, NodeId > > itr = hc.nodes.entrySet().iterator(); itr.hasNext(); ) {
				Map.Entry< BytesWrapper, NodeId > pageEntry = itr.next();
				Harvester.LOG.info( "node: " + pageEntry.getKey() + " size is:" + pageEntry.getValue().paths.size() );
			}
		}
	}
	

	static public void init() {

		// prevent being set more than once
		if( null == mainCache ) synchronized ( Storage.class ) {
			if( null == mainCache ) mainCache = new ConcurrentHashMap< BytesWrapper, HostCache >( Settings.Cache.hosts_per_job );
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
		hostKey = new BytesWrapper( NodeUtil.intToBase64( hostHash ) );

		if ( null == path ){
			path = "/" ;
			LOG.debug("a path with null value change to / value");
		}

		pathHash = NodeUtil.stringToId( path );
		pathKey = new BytesWrapper( NodeUtil.intToBase64( pathHash ) );
		
		hostCache = loadHost( hostKey, hostHash );		
	}

 	public HostCache loadHost( final BytesWrapper key, final Integer hash ) {
		HostCache hc = mainCache.get( key );
		if( null == hc ) /*synchronized( mainCache ) */ {
			// if( null == hostCache ) {
				LOG.info( "loading host from storage" );
			
				hc = loadHostInfo( new HostCache( key, hash ) );
				mainCache.put( key, hc );
				
			// }
		}
		LOG.info( "got host: " + hc );
		return hc;
	}

 	public int addNode( String nodeXpath, Integer nodeHash ) {
 		return hostCache.addNode( nodeXpath, nodeHash, pathKey.getBytes() );
 	}
 	
	public void filterEnd() {
		pageEnd( false );
	}
	
	public void learnEnd() {
		if ( 0 == pageLearnedCounter.incrementAndGet() % Settings.Frequency.gc ) {
			LOG.info( "after learning gc is triggered, counter:" + pageLearnedCounter.intValue() );
			pruneMainCache();				
		}		

		testSaveAndLoad();
		
		pageEnd( true );
	}
	

	public void testSaveAndLoad() {
		//  begin test
		
		LOG.info( "begin test");
		String tHostName = "www.2locos.com";
		LOG.info( "test host:" + tHostName );
		Integer tHostHash = NodeUtil.stringToId( tHostName );
		LOG.info( "test host hash:" + tHostHash );
		BytesWrapper tHostKey = new BytesWrapper( NodeUtil.intToBase64( tHostHash ) );
		
		byte[] tempIntegerBuff = new byte[ Integer.BYTES * 2 ];
		
		NodeUtil.decoder.decode( tHostKey.getBytes(), tempIntegerBuff );
		int tHostHash2 = ByteBuffer.wrap( tempIntegerBuff ).getInt( );
		LOG.info( "test host hash2:" + tHostHash2 );
		
		HostCache h1 = new HostCache( tHostKey.getBytes() );
		LOG.info( "h1:" + h1 );
		
		HostCache h2 = new HostCache( tHostKey, tHostHash );
		LOG.info( "h2:" + h2 );
		
		LOG.info( "h1 ?= h2 " + h1.equals( h2 ) );
		LOG.info( "h2 ?= h1 " + h2.equals( h1 ) );
		LOG.info( "h1 hash:" + h1.hashCode() );
		LOG.info( "h2 hash:" + h2.hashCode() );
		
		LOG.info( "Begin Test" );
		
		saveHostInfo( hostCache );
		LOG.info( "before test1, hostKey:" + hostKey );
		HostCache test1 = loadHostInfo( new HostCache( hostKey, hostHash ) );
		LOG.info( "before test2, hostKey:" + hostKey );
		HostCache test2 = loadHostInfo( new HostCache( hostKey.getBytes() ) );
		HostCache test3 = loadHostInfo( new HostCache( hostCache.getKey( false ), hostCache.hostHash ) );
		
		LOG.info( "original:" + hostCache );
		LOG.info( "test1:" + test1 );
		LOG.info( "test2:" + test2 );
		LOG.info( "test3:" + test3 );
		LOG.info( "original ?= 1 :" + hostCache.equals( test1 ) );
		LOG.info( "original ?= 2 :" + hostCache.equals( test2 ) );
		LOG.info( "original ?= 3 :" + hostCache.equals( test3 ) );
		LOG.info( "1 ?= 2 :" + test1.equals( test2 ) );
		LOG.info( "2 ?= 3 :" + test2.equals( test3 ) );
		// end test
	}


//	protected void finalize() {
//		System.err.println( "Storage finalize was called" );
//		LOG.info( "Storage finalize was called." );
//	}
	

	protected void pruneMainCache() {
		for( Map.Entry< BytesWrapper, HostCache > entry: mainCache.entrySet() ) {
			BytesWrapper hId = entry.getKey();
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
