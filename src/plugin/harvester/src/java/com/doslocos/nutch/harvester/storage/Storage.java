package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.harvester.PageNodeId;
import com.doslocos.nutch.harvester.NodeValue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {

	static public final Logger LOG = LoggerFactory.getLogger( Storage.class );
	static public int bCounter = 0;

	static protected LRUCache< NodeId, NodeValue > cache;
	static protected int cacheThreshould, batchSize = 0;
	static protected boolean readBackend = true;

	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< PageNodeId, NodeValue> currentPage = new LinkedHashMap< PageNodeId, NodeValue>( 4000, .95f );
	public final Vector< PageNodeId > exclusion = new Vector< PageNodeId>( 50 );

	public int counter = 0;

	public String host;
	public String path;

	public int hostId;
	public int pathId;


	static public void set( Configuration conf ) {

		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 1000  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8f );		
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshold" , 100  );

		batchSize = conf.getInt( "doslocos.harvester.storage.batchsize", 500 );
		LOG.info( "batch size:" + batchSize );
		LOG.info( "Initilizing cache, size:" + cacheSize + ", loadFactor:" + loadFactor );

		if( cacheSize < 1000 ) cacheSize = 1000;
		
		cache = new LRUCache< NodeId, NodeValue >( cacheSize, loadFactor );
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
	public void addNodeToList( String path, int hash ) {
		PageNodeId id = new PageNodeId( stringToId( path ), hash );
		NodeId nid = new NodeId( hostId, id );
		NodeValue val = cache.get( nid );

		currentPage.put( id, val );

		if( null == val && readBackend  ) {
			addToBackendList( nid );
		} else {
			exclusion.add( id );
		}

	}


	public Map<PageNodeId, NodeValue> getAllFreq() {
		//		for( Map.Entry<PageNodeId, NodeValue> entry : currentPage.entrySet() ) {
		//			NodeValue val = cache.get( new NodeId( hostId, entry.getKey() ) );
		//			try {
		//				if( null == val ) {
		//					// prepare for database
		//					addToBackendList( entry.getKey() ) ;
		//
		//				} else {
		//					entry.setValue( val );
		//				}
		//			} catch( Exception e ) {
		//				LOG.error( "I don't know either: ", e );
		//			}
		//		}

		
		
		Log.info( "page cache size:" + currentPage.size() );
		// TODO check the cache tershold
		currentPage.putAll( getBackendFreq() );

		Log.info( "page cache size:" + currentPage.size() );
		return currentPage;
	}




	/**
	 * 
	 * @param hostId
	 * @param pathId
	 * @param hash
	 * @param xpath
	 * @return boolean return false if node already existed, true if it has been added
	 */
	// public abstract boolean addNode( String xpath, int hash );

	// public abstract int getNodeFreq( int hostId, int hash, String xpath );

	public abstract boolean pageEnd();

	protected abstract void addToBackendList( PageNodeId id );
	protected abstract void addToBackendList( NodeId id );

	protected abstract Map< PageNodeId, NodeValue > getBackendFreq();
}
