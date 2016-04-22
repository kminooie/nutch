package com.doslocos.nutch.harvester;


import org.apache.hadoop.conf.Configuration;
import com.doslocos.nutch.util.LRUCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Storage {
	public int counter = 0;
	public static int bCounter=0;

	public String host;
	public String path;

	public Integer hostId;
	public Integer pathId;

	protected static cache;
	protected static cacheThreshould;

	public static final Logger LOG = LoggerFactory.getLogger( Storage.class );


	public static void set( Configuration conf ) {
		cacheThreshould =  conf.getInt( "doslocos.harvester.cache.threshould" , 100  );
		int cacheSize = conf.getInt( "doslocos.harvester.cache.size" , 0  );
		float loadFactor = conf.getFloat( "doslocos.harvester.cache.loadfactor" , 0.8 );

		if( cacheSize > 0 ) {
			cache = new LRUCache( cacheSize, floadFactor );

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
	 * 
	 * @param hostId
	 * @param pathId
	 * @param hash
	 * @param xpath
	 * @return boolean return false if node already existed, true if it has been added
	 */
	public abstract boolean addNode( int hostId, int pathId, int hash, String xpath );
	
	public abstract int getNodeFreq( int hostId, int hash, String xpath );
	
	public abstract boolean emptyBatch(int pathId);
}

protected class node {
	public Integer hash;
	public Integer xpathID;
	public Integer fq;
}