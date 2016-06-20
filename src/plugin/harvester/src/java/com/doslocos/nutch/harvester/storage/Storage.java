package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;
import com.doslocos.nutch.harvester.Harvester;
import com.doslocos.nutch.harvester.NodeId;
import com.doslocos.nutch.harvester.NodeValue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );

	static protected LRUCache< HostCache, NodeValue > cache;
	static protected Set<Integer> cleanupHostIds = null;
	static protected int cacheThreshould = 0;
	static protected boolean readBackend = true;

	static protected AtomicInteger pCounter = null;
	static protected int cleanUpInterval = 0;


	public final Vector< NodeId > missing = new Vector< NodeId>( 2048 );

	public int counter = 0;

	public String host, path;

	public Integer hostId = 0, pathId = 0;
	
	protected LRUCache< String, NodeId > hostCache; 

	private int cacheHit = 0, cacheMissed = 0;

	static synchronized public void setConf( Configuration conf ) {

		// prevent being set more than once
		if( null != pCounter ) return;
		
		pCounter = new AtomicInteger();
		
		
		boolean cleanUpEnalbed = conf.getBoolean( "doslocos.harvester.storage.cleanup", false );
		int rangeMin = conf.getInt( "doslocos.harvester.storage.cleanup.min", 1322 );
		int rangeMax = conf.getInt( "doslocos.harvester.storage.cleanup.max", 2893 );
		if( cleanUpEnalbed ) {
			cleanUpInterval = rangeMin + (int)( Math.random() * Math.abs( (rangeMax - rangeMin) + 1) );
			cleanupHostIds = new HashSet<Integer>( cleanUpInterval );
			LOG.info( "clean up enabled. interval:" + cleanUpInterval );
		} else {
			LOG.info( "Storage cleanup disabled." );
		}

	}

	
	public Storage( String host, String path ) {
		this.host = host;
		hostId = NodeUtil.stringToId( host );

		if ( null == path ){
			path = "/" ;
			LOG.debug("a path with null value change to / value");

		}
		this.path = path;
		pathId = NodeUtil.stringToId( path );
		
		loadHost( hostId );
		
	}

	public void loadHost( Integer hostId ) {
		synchronized( Harvester.mainCache ) {
			hostCache = Harvester.mainCache.get( hostId );
			if( null == hostCache ) {
				Harvester.mainCache.put( hostId, hostCache = new LRUCache< String, NodeId >( 4096, .9f ) );
				LOG.info( "creating new hostCache for hostId:" + hostId );
				
				
			}
		}
	}
	
	public void addNodeToList( String xpath, int hash ) {
		NodeId id = new NodeId( NodeUtil.stringToId( xpath ), hash );
		HostCache nid = new HostCache( hostId, id );
		NodeValue val = cache.get( nid );

		if( null == val ) {
			++cacheMissed;

			missing.add( id );
		} else {
			++cacheHit;
			currentPage.put( id, val );
		}
		
		// new stuff
		Set<Integer> nodeSet = hostCache.get( id );
		
		if( null == nodeSet ) {
			hostCache.put(id, nodeSet = Collections.synchronizedSet( new HashSet<Integer>( 2048, .8f ) ) );
			// LOG.info( "allocating new set for pageNodeId:" + id );
		}
		
		nodeSet.add( pathId );
		
	}
	
		
		
	public Map<NodeId, NodeValue> getAllFreq() {
		LOG.debug( "page cache size:" + currentPage.size() );
		Map<NodeId, NodeValue> backendData = getBackendFreq();
		LOG.debug( "number of items read from backend:" + backendData.size() );
		
		for( Map.Entry< NodeId, NodeValue > e:backendData.entrySet() ) {
			NodeValue val = e.getValue();
			
			if( null == val ) {
				LOG.error( "value is null. key:" + e.getKey() );
			}

			if( val.frequency > cacheThreshould ) {
	    		cache.put( new HostCache( hostId, e.getKey() ), val );
	    	}
			
			currentPage.put( e.getKey(),val );
		}
		
		
		
		LOG.info( "page cache size:" + currentPage.size() );
		LOG.info( "hit:" + cacheHit + " missed:" + cacheMissed );
		
		// dumpMainCache();
		return currentPage;
	}


	public void filterEnd() {
		pageEnd( false );
	}
	
	public void learnEnd() {
		if ( ( 0 != cleanUpInterval ) && ( 0 == pCounter.incrementAndGet() % cleanUpInterval ) ) {
			LOG.info( "about to prune main cache. pCounter:" + pCounter );
			Harvester.pruneMainCache();				
		}
		
		if( 0 != cleanUpInterval ) {
			
			cleanupHostIds.add( hostId );
			
			if ( 0 == pCounter.intValue() % cleanUpInterval ) {
				LOG.info( "cleanUpDb called. pCounter:" + pCounter );
				cleanUpDb( cleanupHostIds );
				LOG.info( "cleanUp finished." );				
			}
		}		
		
		pageEnd( true );
	}
	
	public void pageEnd( boolean learn ) {
		
	}
	

	protected void finalize() {
		System.err.println( "Storage finalize was called" );
		LOG.info( "Storage finalize was called." );
	}
	
	public abstract void incNodeFreq( NodeId id, NodeValue val );

	protected abstract void addToBackendList( NodeId id );
	protected abstract Map< NodeId, NodeValue > getBackendFreq();
	
	protected abstract boolean cleanUpDb( Set<Integer> hostIds ) ;
}
