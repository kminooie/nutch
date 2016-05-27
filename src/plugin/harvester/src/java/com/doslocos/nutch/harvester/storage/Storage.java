package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.Iterator;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );

	static protected LRUCache< NodeId, NodeValue > cache;
	static protected Set<Integer> cleanupHostIds = null;
	static protected int cacheThreshould = 0;
	static protected boolean readBackend = true;

	static protected AtomicInteger pCounter = null;
	static protected int cleanUpInterval = 0, frequency_threshould;

	//new stuff
	static protected ConcurrentHashMap< Integer, LRUCache< PageNodeId, Set< Integer > > > mainCache 
		= new ConcurrentHashMap< Integer, LRUCache< PageNodeId, Set< Integer > > >( 1024 );
	
	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< PageNodeId, NodeValue> currentPage = new LinkedHashMap< PageNodeId, NodeValue>( 4000, .9f );

	public final Vector< PageNodeId > missing = new Vector< PageNodeId>( 2048 );

	public int counter = 0;

	public String host, path;

	public Integer hostId = 0, pathId = 0;

	private int cacheHit = 0, cacheMissed = 0;

	static synchronized public void set( Configuration conf ) {

		// prevent being set more than once
		if( null != pCounter ) return;
		
		pCounter = new AtomicInteger();
		
		frequency_threshould =  conf.getInt( "doslocos.harvester.frequency_threshould" , 2  );
		
		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 1000  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8f );		
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshold" , 100  );
		
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

		LOG.info( "Initilizing cache, size:" + cacheSize + ", loadFactor:" + loadFactor );
		LOG.info( "Initilizing cache threshold : " +cacheThreshould );

		if( cacheSize < 1000 ) {
			cacheSize = 1000;
			LOG.info( "over writing cahce size to " +  cacheSize );
		}
		
		cache = new LRUCache< NodeId, NodeValue >( cacheSize, loadFactor );
	}

	static public int stringToId( String str ) {
		return str.hashCode();
	}

	public Storage( String host, String path ) {
		this.host = host;
		hostId = stringToId( host );

		if ( null == path ){
			path = "/" ;
			LOG.debug("a path with null value change to / value");

		}
		this.path = path;
		pathId = stringToId( path );
		
		// new stuff 
		if( null == mainCache.get( hostId ) ) {
			mainCache.put( hostId, new LRUCache< PageNodeId, Set< Integer > >( 4096, .9f ) );
		}
		
		
	}


	public void addNodeToList( String xpath, int hash ) {
		PageNodeId id = new PageNodeId( stringToId( xpath ), hash );
		NodeId nid = new NodeId( hostId, id );
		NodeValue val = cache.get( nid );

		if( null == val ) {
			++cacheMissed;

			missing.add( id );
		} else {
			++cacheHit;
			currentPage.put( id, val );
		}
		
		// new stuff
		Set<Integer> nodeSet = mainCache.get( hostId ).get( id );
		
		if( null == nodeSet ) {
			mainCache.get( hostId ).put(id, Collections.synchronizedSet( new HashSet<Integer>( 2048, .8f ) ) );
			nodeSet = mainCache.get( hostId ).get( id );
		}
		
		nodeSet.add( pathId );
		
	}

	
	public void dumpMainCache() {
		for( Map.Entry< Integer, LRUCache< PageNodeId, Set< Integer > > > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			LRUCache< PageNodeId, Set< Integer > > hostCache = entry.getValue();
			
			LOG.info( "hostId: " + hId );
			for( Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.getAll().iterator(); itr.hasNext(); ) {
				Map.Entry< PageNodeId, Set< Integer > > pageEntry = itr.next();
				LOG.info( "node: " + pageEntry.getKey() + " size is:" + pageEntry.getValue().size() );
			}
		}
	}

	
	public void pruneMainCache() {
		for( Map.Entry< Integer, LRUCache< PageNodeId, Set< Integer > > > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			LRUCache< PageNodeId, Set< Integer > > hostCache = entry.getValue();
			
			LOG.info( "pruning hostId: " + hId + " with size " + hostCache.usedEntries() );
			
			// Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.getAll().iterator();
			Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.entrySet().iterator();			
			
			
			while( itr.hasNext() ) {
				Map.Entry< PageNodeId, Set< Integer > > pageEntry = itr.next();
				if( pageEntry.getValue().size() < frequency_threshould ) {
					LOG.info( "removing node: " + pageEntry.getKey() + " with size:" + pageEntry.getValue().size() );
					itr.remove();
				}				
				
			}
			
			LOG.info( "size after pruning: " + hostCache.usedEntries() );
		}
	}	
	
	
	
	
	public Map<PageNodeId, NodeValue> getAllFreq() {
		LOG.debug( "page cache size:" + currentPage.size() );
		Map<PageNodeId, NodeValue> backendData = getBackendFreq();
		LOG.debug( "number of items read from backend:" + backendData.size() );
		
		for( Map.Entry< PageNodeId, NodeValue > e:backendData.entrySet() ) {
			NodeValue val = e.getValue();
			
			if( null == val ) {
				LOG.error( "value is null. key:" + e.getKey() );
			}

			if( val.frequency > cacheThreshould ) {
	    		cache.put( new NodeId( hostId, e.getKey() ), val );
	    	}
			
			currentPage.put( e.getKey(),val );
		}
		
		
		
		LOG.info( "page cache size:" + currentPage.size() );
		LOG.info( "hit:" + cacheHit + " missed:" + cacheMissed );
		
		dumpMainCache();
		return currentPage;
	}


	public void filterEnd() {
		pageEnd( false );
	}
	
	public void learnEnd() {
		pageEnd( true );
	}
	
	public void pageEnd( boolean learn ) {
		pCounter.incrementAndGet();
		
		if ( learn && ( cleanUpInterval != 0 ) && ( 0 == pCounter.intValue() % cleanUpInterval ) ) {
			LOG.info( "cleanUpDb called. pCounter:" + pCounter );
			pruneMainCache();				
		}
		
		if( learn && ( cleanUpInterval != 0 )  ) {
			
			cleanupHostIds.add( hostId );
			
			if ( 0 == pCounter.intValue() % cleanUpInterval ) {
				LOG.info( "cleanUpDb called. pCounter:" + pCounter );
				cleanUpDb( cleanupHostIds );
				LOG.info( "cleanUp finished." );				
			}
		}
		
		
		
	}
	
	public abstract void incNodeFreq( PageNodeId id, NodeValue val );

	protected abstract void addToBackendList( PageNodeId id );
	protected abstract Map< PageNodeId, NodeValue > getBackendFreq();
	
	protected abstract boolean cleanUpDb( Set<Integer> hostIds ) ;
}
