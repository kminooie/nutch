/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import com.doslocos.nutch.util.NodeUtil;

public class NodeId {

	static public final int BYTES = 2 * Integer.BYTES;
	static public int MAX_NUM_PATHS = 128;
	
	public int xpathId;
	public int hash;
	
	public final List<Integer> paths = Collections.synchronizedList( new ArrayList<Integer>( NodeId.MAX_NUM_PATHS ) );
	public volatile int numberOfPaths = 0;
	
	
	public NodeId( int xpathId, int hash ) {
		this.xpathId = xpathId;
		this.hash = hash;
	}

	public NodeId( String xpath, int hash ) {
		this.xpathId = NodeUtil.stringToId( xpath );
		this.hash = hash;
	}
	
	public NodeId( NodeId id ) {
		xpathId = id.xpathId;
		hash = id.hash;
		numberOfPaths = id.numberOfPaths;
		synchronized ( id.paths ) {
			paths.addAll( id.paths );
		}		
	}
	
	public NodeId( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		xpathId = wrapped.getInt();
		hash = wrapped.getInt();		
	}
	
	public NodeId( String key ) {
		this( NodeUtil.decoder.decode( key ) );
	}

	
	public byte[] getBytes() {
		return  ByteBuffer.allocate( NodeId.BYTES ).putInt( xpathId ).putInt( hash ).array();
	}
	
	public String getKey() {
		return NodeUtil.encoder.encodeToString( getBytes() );
	}

	public boolean addPath( Integer pathId ) {
		++numberOfPaths;
		
		return paths.add( pathId );
	}
 
	
	@Override
	public int hashCode() {
		return xpathId ^ hash;
	}

	@Override
	public boolean equals( Object obj ) {
		NodeId rhs = ( NodeId ) obj;
		return xpathId == rhs.xpathId && hash == rhs.hash;
	}

	@Override
	public String toString() {
		return "xpath:" + xpathId + " hash:" + hash;
	}
	
	
	// test
	static public void main( String args[] ) {
		NodeId id = new NodeId( 25, -1362 );
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
