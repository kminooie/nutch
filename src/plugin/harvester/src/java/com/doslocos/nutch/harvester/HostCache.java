/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;


public class HostCache {

	static public final Logger LOG = LoggerFactory.getLogger( HostCache.class );
	static public final int BYTES = Integer.BYTES ;// + NodeId.BYTES;
	
	private final ByteBuffer hostKey;
	private final Integer hostHash;

	public final LRUCache< ByteBuffer, NodeId > nodes;
	public final AtomicInteger pageLearnedCounter = new AtomicInteger( 0 );

	public boolean needPrune = false, needSave = false;
	public int max = 0, min = 0;
	
	public HostCache( Integer hash ) {
		int capacity = Settings.Cache.getInitialCapacity( Math.round(
				Settings.Cache.nodes_per_page * Settings.Frequency.gc * Settings.Cache.load_factor
			) + 1 );
		
		hostHash = hash;
		hostKey = NodeUtil.intToB64BBuffer( hostHash );
		
		nodes = new LRUCache< ByteBuffer, NodeId > ( capacity, Settings.Cache.load_factor );
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


	public byte[] getB64Key( boolean prefix ) {
		return hostKey.array();
	}


	public Integer getKey() {
		return hostHash;
	}


	public String[] getNodesKeys() {
		return nodes.keySet().toArray( new String[0] );
	}
	
	public void pruneNodes() {
		LOG.info( "pruning hostId: " + hostHash + " with size " + nodes.size() );
		
		synchronized( this ) {
			Iterator< Map.Entry< ByteBuffer, NodeId > > itr = nodes.entrySet().iterator();			
			min = max = -1;
			while( itr.hasNext() ) {
				Map.Entry< ByteBuffer, NodeId > entry = itr.next();
				NodeId node = entry.getValue();
				int freq = node.getFrequency();
				
				// LOG.debug( "pruning node: " + node );
				if( freq < Settings.Frequency.collect ) {
					itr.remove();
					// LOG.debug( "removed." );
				} else {
					// LOG.debug( "kept." );
					if( freq > max ) {
						max = freq;
					} else if ( freq < min || -1 == min ) {
						min = freq;
					}
				}
			}
			
			needPrune = false;
		}
		
		LOG.info( "size after pruning: " + nodes.size() + " min:" + min + " max:" + max );
		 
	}


	public int addNode( String nodeXpath, Integer nodeHash, byte[] path ) {
		
		NodeId node = getNode( nodeXpath, nodeHash );
		if( null == node ) {
			synchronized( this ) {
				node = getNode( nodeXpath, nodeHash );
				if( null == node ) {
					node = new NodeId( nodeXpath, nodeHash );
					// LOG.debug( "adding new node key:" + node );
					nodes.put( node.getKey(), node );
				}
			}			
		}
		
		
		node.addPath( path );
		needPrune = needSave = true;
		
//		if( LOG.isDebugEnabled() ) {
//			LOG.debug( "added new path hash:" + new String( path ) );
//			LOG.debug( "to node: " + node + " with frequency:" + node.getFrequency() +" and rencent Freq:" + node.getRecentFrequency() );			
//		}
		
		return node.getFrequency();
	}
	
	public NodeId getNode( String nodeXpath, Integer nodeHash ) {
		int[] params = { NodeUtil.stringToId( nodeXpath ), nodeHash};
		return  nodes.get( NodeUtil.intArrToB64BBuffer( params ) );		
	}
	
	@Override
	public int hashCode() {
		return hostHash.intValue();
	}

	@Override
	public boolean equals( Object obj ) {
		return ( obj instanceof HostCache ) &&  hostHash.equals( ((HostCache)obj).hostHash );
	}

	@Override
	public String toString() {
		return "host:" + hostHash + " encoded:" + new String( hostKey.array() ) + " no. nodes:" + nodes.size()  
			+ " pruned:" + ( ! needPrune ) + " saved:" + ( ! needSave )+ " in buffer:" + hostKey;
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
