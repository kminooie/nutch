/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;



public class CacheItem {
	public Integer hostId;
	public Integer hash;
	public Integer xpathId;
	
	public CacheItem( Integer hostId, Integer xpathId, Integer hash ) {
		this.hostId = hostId;
		this.xpathId = xpathId;
		this.hash = hash;
	}
	
	public CacheItem( Integer hostId, NodeItem ni ) {
		this.hostId = hostId;
		this.xpathId = ni.xpathId;
		this.hash = ni.hash;
	}
	
}
