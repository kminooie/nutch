/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;


public class HostCache {

	static public final Logger LOG = LoggerFactory.getLogger( HostCache.class );
	static public final int BYTES = Integer.BYTES ;// + NodeId.BYTES;
	
	private final ByteBuffer hostKey;
	
	private Integer hostHash;
	//private final byte[] hostKey;
	
	public boolean needPrune = false, needSave = false;
	
	public final LRUCache< ByteBuffer, NodeId > nodes;

	/*	
	public HostCache( Integer hostId ) {
		this.hostHash = hostId;
		nodes = new LRUCache< BytesWrapper, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	public HostCache( String key ) {
		this(  NodeUtil.decoder.decode( key ) );
		this.hostKey = key;		
	}
	*/
	
	
	
	public HostCache( Integer hash ) {
		hostHash = hash;
		hostKey = NodeUtil.intToB64BBuffer( hostHash );
		
		nodes = new LRUCache< ByteBuffer, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	
	public HostCache( byte[] bytes ) {
		hostKey = ByteBuffer.wrap( bytes );
		hostHash = NodeUtil.b64BBufferToInt( hostKey );
		
		nodes = new LRUCache< ByteBuffer, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	public HostCache( ByteBuffer bytes ) {
		hostKey = bytes;
		hostHash = NodeUtil.b64BBufferToInt( hostKey );
		
		nodes = new LRUCache< ByteBuffer, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}

	public ByteBuffer getB64Key( boolean prefix ) {
//		if( prefix ) {
//			return new BytesWrapper( (new BytesWrapper( Settings.Storage.SEPARATOR.getBytes() ).concat( hostKey ) ) );			
//		} else {
//			return hostKey;
//		}
		
		hostKey.clear();
		return hostKey;
	}

	public Integer getKey() {
		return hostHash;
	}


//	public byte[] getBytes() {
//		return  ByteBuffer.allocate( HostCache.BYTES ).putInt( hostHash ).array();
//	}	

	public String[] getNodesKeys() {
		return nodes.keySet().toArray( new String[0] );
	}
	
	public void pruneNodes() {
		LOG.info( "pruning hostId: " + hostHash + " with size " + nodes.size() );
		
		synchronized( nodes ) {
			Iterator< Map.Entry< ByteBuffer, NodeId > > itr = nodes.entrySet().iterator();			
					
			while( itr.hasNext() ) {
				Map.Entry< ByteBuffer, NodeId > entry = itr.next();
				NodeId node = entry.getValue();
				
				if( node.getRecentFrequency() < Settings.Frequency.collect ) {
					LOG.info( "removing node: " + entry.getKey() + " with freq:" + node.getFrequency() + " recent paths:" + node.getRecentFrequency() );
					itr.remove();
				}
			}
			
			needPrune = false;
		}
		
		LOG.info( "size after pruning: " + nodes.size() );
	}


	public int addNode( String nodeXpath, Integer nodeHash, byte[] path ) {
		
		NodeId node = getNode( nodeXpath, nodeHash );
		if( null == node ) {
			// LOG.debug( "adding new node key:" + key );
			node = new NodeId( nodeXpath, nodeHash );
			nodes.put( node.getKey(), node );
		}
		
		// LOG.debug( "adding new path hash:" + pathHash );
		node.addPath( path );
		needPrune = needSave = true;
		
		return node.getFrequency();
	}
	
	public NodeId getNode( String nodeXpath, Integer nodeHash ) {
		int[] params = { NodeUtil.stringToId( nodeXpath ), nodeHash};
		return  nodes.get( NodeUtil.intArrToB64BBuffer( params ) );		
	}
	
	@Override
	public int hashCode() {
		return hostHash; // ^ pageNodeId.hashCode();
		//return key.hashCode();
	}

	@Override
	public boolean equals( Object obj ) {
		return ( obj instanceof HostCache ) &&  hostHash.equals( ((HostCache)obj).hostHash );
	}

	@Override
	public String toString() {
		return "host:" + hostHash + " key:" + hostKey + " with value:" + new String( hostKey.array() ) 
			+ " number of nodes:" + nodes.size() + " pruned:" + ( ! needPrune ) + " saved:" + ( ! needSave );
	}
	
	// test
	static public void main( String args[] ) {
		HostCache id = new HostCache( "0byeeQ".getBytes() );
		System.out.println( "node: " + id );
		
		HostCache id2 = new HostCache( id.hashCode() );
		System.out.println( "node2: " + id2 );
		
		if( id.hashCode() == id2.hashCode() ) {
			System.out.println( "hashCode passed:" + id2.hashCode() );			
		}
		
		if( id.equals( id2 ) ) {
			System.out.println( "equals passed" );			
		}

	}

}
