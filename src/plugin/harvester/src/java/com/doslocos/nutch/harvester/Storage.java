package com.doslocos.nutch.harvester;


import org.apache.hadoop.conf.Configuration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.harvester.NodeItem;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {
	public int counter = 0;
	public static int bCounter=0;

	public String host;
	public String path;

	public Integer hostId;
	public Integer pathId;

	protected static LRUCache< CacheItem, Integer > cache;
	protected static int cacheThreshould;

	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public LinkedHashMap< NodeItem, Integer> currentMap = new LinkedHashMap< NodeItem, Integer>( 4000, .95f );
	public static final Logger LOG = LoggerFactory.getLogger( Storage.class );


	public static void set( Configuration conf ) {
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshould" , 100  );
		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 0  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8f );

		if( cacheSize > 0 ) {
			cache = new LRUCache< CacheItem, Integer >( cacheSize, loadFactor );
		}

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

	public static int stringToId( String str ) {
		return str.hashCode();
	}
	
	
	/**
	 * would be called for every node in a given page
	 * @param xpath 
	 * @param hash node hash code
	 */
	public void addNodeToList( Integer xpath, Integer hash ) {
		currentMap.put( new NodeItem( xpath, hash ), null );
	}
	
	public Map<NodeItem, Integer> getAllFreq() {
		for( Map.Entry<NodeItem, Integer> entry : currentMap.entrySet() ) {
			Integer val = cache.get( new CacheItem( hostId, entry.getKey() ) );
			try {
				if( null == val ) {
					// prepare for database
					addToBackendList( entry.getKey().xpathId, entry.getKey().hash ) ;

				} else {
					entry.setValue( val );
				}
			} catch( Exception e ) {
				LOG.error( "I don't know either: ", e );
			}
		}
		
		// TODO check the cache tershold
		currentMap.putAll( getBackendFreq() );
		
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
	public abstract boolean addNode( int hostId, int pathId, int hash, String xpath );
	
	public abstract int getNodeFreq( int hostId, int hash, String xpath );
	
	public abstract boolean pageEnd();
	
	protected abstract void addToBackendList( Integer xpath, Integer hash );
	protected abstract Map<NodeItem, Integer> getBackendFreq();



}
