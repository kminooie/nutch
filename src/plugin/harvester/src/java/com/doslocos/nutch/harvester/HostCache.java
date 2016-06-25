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

import com.doslocos.nutch.util.BytesWrapper;
import com.doslocos.nutch.util.LRUCache;


public class HostCache {

	static public final Logger LOG = LoggerFactory.getLogger( HostCache.class );
	static public final int BYTES = Integer.BYTES ;// + NodeId.BYTES;
	
	
	public final Integer hostHash;
	private final BytesWrapper hostKey;
	
	public boolean needPrune = false, needSave = false;
	
	public final LRUCache< BytesWrapper, NodeId > nodes;

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
	
	public HostCache( BytesWrapper bytes, Integer hash ) {
		hostKey = bytes;
		hostHash = hash;

		nodes = new LRUCache< BytesWrapper, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	
	public HostCache( byte[] bs ) {
		ByteBuffer wrapped = ByteBuffer.wrap( bs );
		hostHash = wrapped.getInt();
		hostKey = new BytesWrapper( bs );
		
		nodes = new LRUCache< BytesWrapper, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}

	
	public BytesWrapper getKey( boolean prefix ) {
		if( prefix ) {
			return new BytesWrapper( (new BytesWrapper( Settings.Storage.SEPARATOR.getBytes() ).concat( hostKey ) ) );			
		} else {
			return hostKey;
		}		
	}

	public byte[] getBytes() {
		return  ByteBuffer.allocate( HostCache.BYTES ).putInt( hostHash ).array();
	}	

	public String[] getNodesKeys() {
		return nodes.keySet().toArray( new String[0] );
	}
	
	public void pruneNodes() {
		LOG.info( "pruning hostId: " + hostHash + " with size " + nodes.size() );
		
		synchronized( nodes ) {
			Iterator< Map.Entry< BytesWrapper, NodeId > > itr = nodes.entrySet().iterator();			
					
			while( itr.hasNext() ) {
				Map.Entry< BytesWrapper, NodeId > entry = itr.next();
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
		BytesWrapper key = NodeId.makeBytesKey( nodeXpath, nodeHash);
		NodeId node = nodes.get( key );
		if( null == node ) {
			// LOG.debug( "adding new node key:" + key );
			node = new NodeId( nodeXpath, nodeHash );
			nodes.put( key, node );
		}
		
		// LOG.debug( "adding new path hash:" + pathHash );
		node.addPath( path );
		needPrune = needSave = true;
		
		return node.getFrequency();
	}
	
	public NodeId getNode( String nodeXpath, Integer nodeHash ) {
		return  nodes.get( NodeId.makeBytesKey( nodeXpath, nodeHash) );		
	}
	
	@Override
	public int hashCode() {
		return hostHash; // ^ pageNodeId.hashCode();
		//return key.hashCode();
	}

	@Override
	public boolean equals( Object obj ) {
		return obj instanceof HostCache && ( (HostCache)obj ).hostHash == hostHash;
	}

	@Override
	public String toString() {
		return "host:" + hostHash + " key:" + hostKey + " number of nodes:" + nodes.size() + " pruned:" + ( ! needPrune ) + " saved:" + ( ! needSave );
	}
	
	// test
	static public void main( String args[] ) {
		HostCache id = new HostCache( "0byeeQ".getBytes() );
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
