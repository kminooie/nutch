/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester.storage;

import java.nio.ByteBuffer;

import com.doslocos.nutch.harvester.NodeId;

public class HostCache {

	static public final int BYTES = Integer.BYTES + NodeId.BYTES;
	
	
	public int hostId;
	public NodeId pageNodeId;
	
	public HostCache( int hostId, int xpathId, int hash ) {
		this.hostId = hostId;
		pageNodeId = new NodeId( xpathId, hash );
	}

	
	public HostCache( int hostId, NodeId pni ) {
		this.hostId = hostId;
		this.pageNodeId = pni;
	}

	
	public HostCache( HostCache hid ) {
		this.hostId = hid.hostId;
		this.pageNodeId = hid.pageNodeId;
	}
	
	
	public HostCache( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		hostId = wrapped.getInt();
		pageNodeId = new NodeId( 0, 0 );
		
		pageNodeId.xpathId = wrapped.getInt();
		pageNodeId.hash = wrapped.getInt();
	}
	
	
	public byte[] getBytes() {
		return  ByteBuffer.allocate( HostCache.BYTES ).putInt( hostId ).put( pageNodeId.getBytes() ).array();
	}	

	
	@Override
	public int hashCode() {
		return hostId ^ pageNodeId.hashCode();
	}

	
	@Override
	public boolean equals( Object obj ) {
		HostCache rhs = ( HostCache ) obj;
		return hostId == rhs.hostId && pageNodeId.equals( rhs.pageNodeId );
	}
	
	
	@Override
	public String toString() {
		return "host:" + hostId + " " + pageNodeId;
	}
	
	// test
	static public void main( String args[] ) {
		HostCache id = new HostCache( 25, -1362, 3223678 );
		System.out.println( "node: " + id );
		
		HostCache id2 = new HostCache( id.getBytes() );
		System.out.println( "node2: " + id2 );
		
		if( id.hashCode() == id2.hashCode() ) {
			System.out.println( "hashCode passed:" + id2.hashCode() );			
		}
		
		if( id.equals( id2 ) ) {
			System.out.println( "equals passed" );			
		}
	}
}
