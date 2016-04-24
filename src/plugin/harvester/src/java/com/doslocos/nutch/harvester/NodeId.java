/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;



public class NodeId {

	public int hostId;
	public int xpathId;
	public int hash;
	
	public NodeId( int hostId, int xpathId, int hash ) {
		this.hostId = hostId;
		this.xpathId = xpathId;
		this.hash = hash;
	}
	
	public NodeId( int hostId, NodeItem ni ) {
		this.hostId = hostId;
		this.xpathId = ni.xpathId;
		this.hash = ni.hash;
	}

	public int hashCode() {
		return hostId ^ xpathId ^ hash;
	}

	public boolean equal( Object obj ) {
		NodeId rhs = ( NodeId ) obj;
		return hosttId == rhs.hosttId && xpathId == rhs.xpathId && hash = rhs.hash;
	}
	
}
