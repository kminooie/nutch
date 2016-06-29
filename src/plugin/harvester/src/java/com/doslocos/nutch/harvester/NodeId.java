/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.util.NodeUtil;


public class NodeId {

	static public final Logger LOG = LoggerFactory.getLogger( NodeId.class );
	static public final int BYTES = 2 * Integer.BYTES;
	static public final int NUM_PATHS = 128;
	
	static private final int XPATH_HASH = 0;
	static private final int NODE_HASH = 1;
	
	//static private final byte[] tempKeyBuff = new byte[ NodeId.BYTES ];
	
	// public final List< byte[] > paths = Collections.synchronizedList( new ArrayList< byte[] >( NUM_PATHS ) );
	private final List< byte[] > paths = new ArrayList< byte[] >( NUM_PATHS );
	
 
	private int[] hashes;
	private ByteBuffer key; 
	private int numSavedPath = 0;
	
//	
//	static public String makeStringKey( String nodeXpath, int nodeHash ) {
//		return NodeUtil.encoder.encodeToString( makeByteArrayId( nodeXpath, nodeHash) );		
//	}
	
//	static public BytesWrapper makeBytesKey( String nodeXpath, int nodeHash ) {
//		synchronized( tempKeyBuff ) {
//			ByteBuffer.wrap( tempKeyBuff ).putInt( NodeUtil.stringToId( nodeXpath ) ).putInt( nodeHash );
//			return new BytesWrapper( NodeUtil.encoder.encode( tempKeyBuff ) );
//		}
//	}
//	
//	static public byte[] makeByteArrayId( String nodeXpath, int nodeHash ) {
//		return ByteBuffer.allocate( NodeId.BYTES ).putInt( NodeUtil.stringToId( nodeXpath ) ).putInt( nodeHash ).array();
//	}



	public NodeId( int xpathHash, int nodeHash ) {
		hashes = new int[2];
		hashes[XPATH_HASH] = xpathHash;
		hashes[NODE_HASH] = nodeHash;

		// this.key = NodeUtil.encoder.encodeToString( getBytes() );
		key = NodeUtil.intArrToB64BBuffer( hashes );
	}

	public NodeId( String xpath, int nodeHash ) {
		this( NodeUtil.stringToId( xpath ), nodeHash );;
	}
	
//	public NodeId( NodeId id ) {
//		xpathHash = id.xpathHash;
//		nodeHash = id.nodeHash;
//		key = id.key;
//		numSavedPath = id.numSavedPath;
//		synchronized ( id.paths ) {
//			paths.addAll( id.paths );
//		}		
//	}
	
	public NodeId( byte bytes[] ) {
		key = ByteBuffer.wrap( bytes );
		hashes = NodeUtil.b64BBufferToIntArr( key );
	}
	
	public NodeId( int freq ,ByteBuffer key ) {
		this.key = key;
		hashes = NodeUtil.b64BBufferToIntArr( key );
		numSavedPath = freq;
	}
	
		

	public NodeId( int freq, byte[] bytes ) {
		this( bytes );
		numSavedPath = freq;
	}
	
	public byte[] getKeyBytes() {
		key.clear();
		return key.array();
	}
	
	public ByteBuffer getKey() {
		key.clear();
		return key;
	}

	/**
	 * should be only used for saving as they discard the internal information
	 */
	public ArrayList< byte[] > getPathsKeys() {
		
		ArrayList< byte[] > result = new ArrayList< byte[] >( paths.size() );
		
		synchronized( paths ) {
			result.addAll( paths );
			paths.clear();
			numSavedPath += result.size();
		}

		return result;
	}
	
	/**
	 * should be only used for saving as they discard the internal information
	 */
	public String[] getPathsKeysStrings() {
		String[] result;
		
		synchronized( paths ) {
			result = paths.toArray( new String[0] );
			paths.clear();
			numSavedPath += result.length;
		}

		return result;
	}
	
	public byte[][] getPathsKeysByteArr() {
		byte[][] result = new byte[0][];
		
		synchronized( paths ) {
			// result = new byte[paths.size()][];
			result =  paths.toArray( result );
			paths.clear();
			numSavedPath += result.length;
		}

		return result;
	}

	public int getFrequency() {
		return numSavedPath + paths.size();
	}

	public int getRecentFrequency() {
		return paths.size();
	}


	public boolean addPath( byte[] path ) {
		boolean result = false;
		
		if( numSavedPath < Settings.Frequency.max ) {
			synchronized( this ) {
				result =  paths.add( path );
			}
		} else if( LOG.isDebugEnabled() ) {
			LOG.info( "ignoring path:" + new String( path ) + " old frequency:" + numSavedPath + " is bigger than MAX" );
		}

		return result;
	}

	
	@Override
	public int hashCode() {
		return Arrays.hashCode( hashes );
	}

	@Override
	public boolean equals( Object obj ) {
		return obj instanceof NodeId && Arrays.equals( hashes, ((NodeId)obj).hashes );
	}

	@Override
	public String toString() {
		return "xpath:" + hashes[XPATH_HASH] + " hash:" + hashes[NODE_HASH] + " key:" + new String( key.array() + " with freq:" + getFrequency() + " recent paths:" + getRecentFrequency() );
	}
	
	/**
	 * java -cp  $( for i in $( ls /2locos/kaveh/nutch/nutch/build/lib/*.jar /2locos/kaveh/nutch/nutch/build/plugins/harvester/*.jar ); do echo -n $i:; done; ). com.doslocos.nutch.harvester.NodeId
	 * @param args
	 */
	
	// test
	static public void main( String args[] ) {
		NodeId id = new NodeId( 25, -1362 );
		System.out.println( "node: " + id );
		
		byte[] b = id.getKey().array();
		
		NodeId id2 = new NodeId( b );
		System.out.println( "node2: " + id2 );
		
		if( id.hashCode() == id2.hashCode() ) {
			System.out.println( "hashCode passed:" + id2.hashCode() );			
		}
		
		if( id.equals( id2 ) ) {
			System.out.println( "equals passed" );			
		}
	}
} 
