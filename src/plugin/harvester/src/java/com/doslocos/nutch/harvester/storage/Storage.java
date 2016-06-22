package com.doslocos.nutch.harvester.storage;


import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.doslocos.nutch.util.NodeUtil;
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
	
	
	public String host, path;

	public Integer hostHash = 0, pathHash = 0;
	
	public HostCache hostCache; 

	

	static public synchronized void init() {

		// prevent being set more than once
		if( null == mainCache ) {
			mainCache = new ConcurrentHashMap< Integer, HostCache >( Settings.Cache.hosts_per_job );
		} else {
			LOG.error( "Seems like Init was called more than once." );
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
	
	static protected void pruneMainCache2() {
		for( Map.Entry< Integer, HostCache > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			HostCache hostCache = entry.getValue();
			
			Storage.LOG.info( "pruning hostId: " + hId + " with size " + hostCache.nodes.size() );
			
			// Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.getAll().iterator();
			Iterator< Map.Entry< String, NodeId > > itr = hostCache.nodes.entrySet().iterator();			
			
			
			while( itr.hasNext() ) {
				Map.Entry< String, NodeId > pageEntry = itr.next();
				if( pageEntry.getValue().paths.size() < Settings.FThreshold.collect ) {
					Storage.LOG.info( "removing node: " + pageEntry.getKey() + " with size:" + pageEntry.getValue().paths.size() );
					itr.remove();
				}				
				
			}
			
			LOG.info( "size after pruning: " + hostCache.nodes.size() );
		}
	} 


 	public Storage( String host, String path ) {
		this.host = host;
		hostHash = NodeUtil.stringToId( host );

		if ( null == path ){
			path = "/" ;
			LOG.debug("a path with null value change to / value");

		}
		this.path = path;
		pathHash = NodeUtil.stringToId( path );
		
		hostCache = loadHost( hostHash );
		
	}

	public HostCache loadHost( Integer hostId ) {
		HostCache hostCache = Storage.mainCache.get( hostId );
		if( null == hostCache ) /*synchronized( Storage.mainCache ) */ {
			// if( null == hostCache ) {
				hostCache = loadHostInfo( new HostCache( hostId ) );
				Storage.mainCache.put( hostId, hostCache );
				LOG.info( "Loaded host: " + hostCache );
			// }
		}
		return hostCache;
	}

	

	public void filterEnd() {
		pageEnd( false );
	}
	
	public void learnEnd() {
		if ( 0 == pageLearnedCounter.incrementAndGet() % Settings.FThreshold.gc ) {
			LOG.info( "after learning gc is triggered, counter:" + pageLearnedCounter.intValue() );
			pruneMainCache();				
		}		
		
		pageEnd( true );
	}
	
	public void pageEnd( boolean learn ) {
		
	}
	

	protected void finalize() {
		System.err.println( "Storage finalize was called" );
		LOG.info( "Storage finalize was called." );
	}
	
	abstract public HostCache loadHostInfo( HostCache hostCache );
	
	abstract public void saveHostInfo( HostCache hostCache );
	
}
