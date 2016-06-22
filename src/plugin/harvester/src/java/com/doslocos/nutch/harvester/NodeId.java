/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

import com.doslocos.nutch.util.NodeUtil;

public class NodeId {

	static public final Logger LOG = LoggerFactory.getLogger( NodeId.class );
	static public final int BYTES = 2 * Integer.BYTES;
	static public final int NUM_PATHS = 128;
	
	public int xpathId, hash;
	public String key; 
	
	public final List<String> paths = Collections.synchronizedList( new ArrayList<String>( NUM_PATHS ) );
	
	public int numSavedPath = 0;
	
	static public String makeKey( String nodeXpath, int nodeHash ) {
		return NodeUtil.encoder.encodeToString( makeBytes( nodeXpath, nodeHash) );
	}
	
	static public byte[] makeBytes( String nodeXpath, int nodeHash ) {
		return ByteBuffer.allocate( NodeId.BYTES ).putInt( nodeXpath.hashCode() ).putInt( nodeHash ).array();
	}
	
	public NodeId( int xpathId, int hash ) {
		this.xpathId = xpathId;
		this.hash = hash;
		this.key = NodeUtil.encoder.encodeToString( getBytes() );
	}

	public NodeId( String xpath, int hash ) {
		this( NodeUtil.stringToId( xpath ), hash );;
	}
	
	public NodeId( NodeId id ) {
		xpathId = id.xpathId;
		hash = id.hash;
		numSavedPath = id.numSavedPath;
		synchronized ( id.paths ) {
			paths.addAll( id.paths );
		}		
	}
	
	public NodeId( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		xpathId = wrapped.getInt();
		hash = wrapped.getInt();
		key = NodeUtil.encoder.encodeToString( wrapped.array() );
	}
	
	public NodeId( String key ) {
		this( NodeUtil.decoder.decode( key ) );
		
		if( this.key.equals( key ) ) {
			LOG.info( "sanity passes." );
		} else {
			LOG.error( "sanity failes." );
		}
	}

	
	public byte[] getBytes() {
		return  ByteBuffer.allocate( NodeId.BYTES ).putInt( xpathId ).putInt( hash ).array();
	}
	
	public String getKey() {
		return key;
	}

	public boolean addPath( Integer pathId ) {
		boolean result = false;
		
		if( numSavedPath < Settings.FThreshold.max ) {
			result =  paths.add( NodeUtil.intToBase64( pathId ) );
		}

		return result;
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
