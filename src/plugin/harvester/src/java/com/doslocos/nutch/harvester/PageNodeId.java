/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;

import com.doslocos.nutch.harvester.storage.Storage;

public class PageNodeId {

	static public final int BYTES = 2 * Integer.BYTES;
	
	public int xpathId;
	public int hash;
	
	
	public PageNodeId( int xpathId, int hash ) {
		this.xpathId = xpathId;
		this.hash = hash;
	}


	public PageNodeId( String xpath, int hash ) {
		this.xpathId = Storage.stringToId( xpath );
		this.hash = hash;
	}
	

	public PageNodeId( PageNodeId id ) {
		this.xpathId = id.xpathId;
		this.hash = id.hash;
	}
	
	public PageNodeId( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		xpathId = wrapped.getInt();
		hash = wrapped.getInt();		
	}

	
	public byte[] getBytes() {
		return  ByteBuffer.allocate( PageNodeId.BYTES ).putInt( xpathId ).putInt( hash ).array();
	}
	
	
	@Override
	public int hashCode() {
		return xpathId ^ hash;
	}

	@Override
	public boolean equals( Object obj ) {
		PageNodeId rhs = ( PageNodeId ) obj;
		return xpathId == rhs.xpathId && hash == rhs.hash;
	}
	

	@Override
	public String toString() {
		return "xpath:" + xpathId + " hash:" + hash;
	}
	
	
	// test
	static public void main( String args[] ) {
		PageNodeId id = new PageNodeId( 25, -1362 );
		System.out.println( "node: " + id );
		
		PageNodeId id2 = new PageNodeId( id.getBytes() );
		System.out.println( "node2: " + id2 );
		
		if( id.hashCode() == id2.hashCode() ) {
			System.out.println( "hashCode passed:" + id2.hashCode() );			
		}
		
		if( id.equals( id2 ) ) {
			System.out.println( "equals passed" );			
		}
	}
} 
