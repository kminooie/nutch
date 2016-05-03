package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Vector;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );

	static protected LRUCache< NodeId, NodeValue > cache;
	static protected Set<Integer> cleanupHostIds = null;
	static protected int cacheThreshould, batchSize = 0;
	static protected boolean readBackend = true;

	static protected int pCounter = 0;
	static protected int cleanUpInterval = 0;

	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< PageNodeId, NodeValue> currentPage = new LinkedHashMap< PageNodeId, NodeValue>( 4000, .9f );

	public final Vector< PageNodeId > missing = new Vector< PageNodeId>( 2048 );

	public int counter = 0;

	public String host;
	public String path;

	public int hostId;
	public int pathId;

	private int cacheHit = 0, cacheMissed = 0;

	static public void set( Configuration conf ) {

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
		}

		batchSize = conf.getInt( "doslocos.harvester.storage.batchsize", 1000 );
		
		LOG.info( "batch size:" + batchSize );
		LOG.info( "Initilizing cache, size:" + cacheSize + ", loadFactor:" + loadFactor );
		LOG.info( "Initilizing cache threshold : " +cacheThreshould );

		if( cacheSize < 1000 ) cacheSize = 1000;
		
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
		return currentPage;
	}


	public void pageEnd( boolean learn ) {
		if( learn ) {
			++pCounter;
			cleanupHostIds.add( hostId );
			
			if (( cleanUpInterval != 0 ) && 0 == pCounter % cleanUpInterval ) {
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
