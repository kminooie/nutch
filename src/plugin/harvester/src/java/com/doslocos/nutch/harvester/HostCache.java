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
	
	
	public final int hostId;
	private String key;
	
	public boolean needPrune = false, needSave = false;
	
	public LRUCache< String, NodeId > nodes;

		
	public HostCache( int hostId ) {
		this.hostId = hostId;
		nodes = new LRUCache< String, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	public HostCache( String key ) {
		this(  NodeUtil.decoder.decode( key ) );
		this.key = key;		
	}
	
	public HostCache( byte b[] ) {
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		hostId = wrapped.getInt();
		nodes = new LRUCache< String, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}

	
	public String getKey( boolean preFix ) {
		if( null == key ) {
			key = NodeUtil.encoder.encodeToString( getBytes() );
		}
		return ( preFix ? Settings.Storage.SEPARATOR : null ) + key;
	}

	public byte[] getBytes() {
		return  ByteBuffer.allocate( HostCache.BYTES ).putInt( hostId ).array();
	}	

	public String[] getNodesKeys() {
		return nodes.keySet().toArray( new String[0] );
	}
	
	public void pruneNodes() {
		LOG.info( "pruning hostId: " + hostId + " with size " + nodes.size() );
		
		synchronized( nodes ) {
			Iterator< Map.Entry< String, NodeId > > itr = nodes.entrySet().iterator();			
					
			while( itr.hasNext() ) {
				Map.Entry< String, NodeId > entry = itr.next();
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


	public /*synchronized*/ int addNode( String nodeXpath, Integer nodeHash, Integer pathHash ) {
		String key = NodeId.makeKey( nodeXpath, nodeHash);
		NodeId node = nodes.get( key );
		if( null == node ) {
			// LOG.debug( "adding new node key:" + key );
			node = new NodeId( nodeXpath, nodeHash );
			nodes.put( key, node );
		}
		
		// LOG.debug( "adding new path hash:" + pathHash );
		node.addPath( pathHash );
		needPrune = needSave = true;
		
		return node.getFrequency();
	}
	
	public NodeId getNode( String nodeXpath, Integer nodeHash ) {
		String key = NodeId.makeKey( nodeXpath, nodeHash);
		return  nodes.get( key );		
	}
	
	@Override
	public int hashCode() {
		return hostId; // ^ pageNodeId.hashCode();
		//return key.hashCode();
	}

	@Override
	public boolean equals( Object obj ) {
		HostCache rhs = ( HostCache ) obj;
		return hostId == rhs.hostId 
			&& key.equals( rhs.key )
			&& nodes.size() == rhs.nodes.size()
		;
	}

	@Override
	public String toString() {
		return "host:" + hostId + " key:" + key + " number of nodes:" + nodes.size() + " pruned:" + ( ! needPrune ) + " saved:" + ( ! needSave );
	}
	
	// test
	static public void main( String args[] ) {
		HostCache id = new HostCache( "0byeeQ" );
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
