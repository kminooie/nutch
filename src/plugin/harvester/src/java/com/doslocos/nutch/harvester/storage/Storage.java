package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;
import com.doslocos.nutch.harvester.Harvester;
import com.doslocos.nutch.harvester.HostCache;
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

	//new stuff
	// static public ConcurrentHashMap< Integer, LRUCache< String, NodeId> > mainCache;
	static public ConcurrentHashMap< Integer, HostCache > mainCache;

	static public synchronized void setConf( Configuration conf ) {

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

	public HostCache loadHost( Integer hostId ) {
		HostCache hostCache = null;
		synchronized( Storage.mainCache ) {
			hostCache = Storage.mainCache.get( hostId );
			if( null == hostCache ) {
				hostCache = loadHostInfo( new HostCache( hostId ) );
				Storage.mainCache.put( hostId, hostCache );
				LOG.info( "Loaded host: " + hostCache );
			}
		}
		return hostCache;
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
	
	public abstract HostCache loadHostInfo( HostCache hostCache );
	
}
