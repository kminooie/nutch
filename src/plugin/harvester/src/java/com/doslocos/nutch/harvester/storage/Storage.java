package com.doslocos.nutch.harvester.storage;


import org.apache.hadoop.conf.Configuration;
import org.mortbay.log.Log;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
	public final LinkedHashMap< PageNodeId, NodeValue> currentPage = new LinkedHashMap< PageNodeId, NodeValue>( 4000, .9f );
	public final Vector< PageNodeId > exclusion = new Vector< PageNodeId>( 64 );

	public int counter = 0;

	public String host;
	public String path;

	public int hostId;
	public int pathId;


	static public void set( Configuration conf ) {

		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 1000  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8f );		
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshold" , 100  );

		batchSize = conf.getInt( "doslocos.harvester.storage.batchsize", 1000 );
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


	public void addNodeToList( String path, int hash ) {
		PageNodeId id = new PageNodeId( stringToId( path ), hash );
		NodeId nid = new NodeId( hostId, id );
		NodeValue val = cache.get( nid );

		currentPage.put( id, val );

		if( null == val && readBackend  ) {
			addToBackendList( id );
		} else {
			exclusion.add( id );
		}

	}


	public Map<PageNodeId, NodeValue> getAllFreq() {
		Log.info( "page cache size:" + currentPage.size() );
		
		for( Map.Entry< PageNodeId, NodeValue > e:getBackendFreq().entrySet() ) {
			NodeValue val = e.getValue();
			
			if( null == val ) {
				LOG.error( "value is null. key:" + e.getKey() );
			}

			if( val.frequency > cacheThreshould ) {
	    		cache.put( new NodeId( hostId, e.getKey() ), val );
	    	}
			
			currentPage.put( e.getKey(),val );
		}
		
		Log.info( "page cache size:" + currentPage.size() );
		return currentPage;
	}

	
	public abstract void incNodeFreq( PageNodeId id, NodeValue val );
	public abstract boolean pageEnd();

	protected abstract void addToBackendList( PageNodeId id );
	protected abstract Map< PageNodeId, NodeValue > getBackendFreq();
	
}
