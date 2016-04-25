/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester.storage;

import java.nio.ByteBuffer;

import com.doslocos.nutch.harvester.PageNodeId;

public class NodeId {

	static public final int BYTES = Integer.BYTES + PageNodeId.BYTES;
	
	
	public int hostId;
	public PageNodeId pageNodeId;
	
	public NodeId( int hostId, int xpathId, int hash ) {
		this.hostId = hostId;
		pageNodeId = new PageNodeId( xpathId, hash );
	}

	
	public NodeId( int hostId, PageNodeId pni ) {
		this.hostId = hostId;
		this.pageNodeId = pni;
	}

	
	public NodeId( NodeId hid ) {
		this.hostId = hid.hostId;
		this.pageNodeId = hid.pageNodeId;
	}
	
	
	public NodeId( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		hostId = wrapped.getInt();
		pageNodeId = new PageNodeId( 0, 0 );
		
		pageNodeId.xpathId = wrapped.getInt();
		pageNodeId.hash = wrapped.getInt();
	}
	
	
	public byte[] getBytes() {
		return  ByteBuffer.allocate( NodeId.BYTES ).putInt( hostId ).put( pageNodeId.getBytes() ).array();
	}	

	
	@Override
	public int hashCode() {
		return hostId ^ pageNodeId.hashCode();
	}

	
	@Override
	public boolean equals( Object obj ) {
		NodeId rhs = ( NodeId ) obj;
		return hostId == rhs.hostId && pageNodeId.equals( rhs.pageNodeId );
	}
	
	
	@Override
	public String toString() {
		return "host:" + hostId + " " + pageNodeId;
	}
	
	// test
	static public void main( String args[] ) {
		NodeId id = new NodeId( 25, -1362, 3223678 );
		System.out.println( "node: " + id );
		
		NodeId id2 = new NodeId( id.getBytes() );
		System.out.println( "node2: " + id2 );
		
		if( id.hashCode() == id2.hashCode() ) {
			System.out.println( "hashCode passed:" + id2.hashCode() );			
		}
		
		if( id.equals( id2 ) ) {
			System.out.println( "equals passed" );			
		}
	}
}
