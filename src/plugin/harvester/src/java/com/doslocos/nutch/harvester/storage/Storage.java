package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;

import java.util.LinkedHashMap;
import java.util.Map;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.harvester.PageNodeId;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {
	
	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );
	static public int bCounter = 0;
	
	static protected LRUCache< NodeId, Integer > cache;
	static protected int cacheThreshould, batchSize = 0;
	
	
	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< PageNodeId, Integer> currentMap = new LinkedHashMap< PageNodeId, Integer>( 4000, .95f );

	public int counter = 0;
	
	public String host;
	public String path;

	public int hostId;
	public int pathId;

	
	static public void set( Configuration conf ) {
		
		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 0  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8f );		
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshold" , 100  );

		batchSize = conf.getInt( "doslocos.harvester.storage.batchsize", 500 );
		LOG.info( "batch size:" + batchSize );
		LOG.info( "Initilizing cache, size:" + cacheSize + ", loadFactor:" + loadFactor );

		if( cacheSize > 0 ) {
			cache = new LRUCache< NodeId, Integer >( cacheSize, loadFactor );
		}

	}
	
	static public int stringToId( String str ) {
		return str.hashCode();
	}
	
	public Storage( String host, String path ) {
		this.host = host;
		hostId = stringToId( host );

		this.path = path;
		if ( null == path ){
			path = "/" ;
		}
		pathId = stringToId( path );
	}

	
	
	/**
	 * would be called for every node in a given page
	 * @param xpath 
	 * @param hash node hash code
	 */
	public void addNodeToList( Integer xpath, Integer hash ) {
		currentMap.put( new PageNodeId( xpath, hash ), null );
	}
	
	public void addNodeToList( String path, Integer hash ) {
		currentMap.put( new PageNodeId( stringToId( path ), hash ), null );
	}
	
	
	public Map<PageNodeId, Integer> getAllFreq() {
		for( Map.Entry<PageNodeId, Integer> entry : currentMap.entrySet() ) {
			Integer val = cache.get( new NodeId( hostId, entry.getKey() ) );
			try {
				if( null == val ) {
					// prepare for database
					addToBackendList( entry.getKey() ) ;

				} else {
					entry.setValue( val );
				}
			} catch( Exception e ) {
				LOG.error( "I don't know either: ", e );
			}
		}
		Log.info( "page cache size:" + currentMap.size() );
		// TODO check the cache tershold
		currentMap.putAll( getBackendFreq() );
		
		Log.info( "page cache size:" + currentMap.size() );
		return currentMap;
	}
	



	/**
	 * 
	 * @param hostId
	 * @param pathId
	 * @param hash
	 * @param xpath
	 * @return boolean return false if node already existed, true if it has been added
	 */
	// public abstract boolean addNode( String xpath, Integer hash );
	
	// public abstract int getNodeFreq( int hostId, int hash, String xpath );
	
	public abstract boolean pageEnd();
	
	protected abstract void addToBackendList( PageNodeId id );
	protected abstract Map<PageNodeId, Integer> getBackendFreq();



}
