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

	static public final Logger LOG = LoggerFactory.getLogger( NodeId.class );
	static public final int BYTES = Integer.BYTES ;// + NodeId.BYTES;
	
	
	public int hostId;
	public String key;
	
	public boolean needPrune = false, needSave = false;
	
	public LRUCache< String, NodeId > nodes;

	
	private HostCache() {
		nodes = new LRUCache< String, NodeId > ( Settings.Cache.nodes_per_page, Settings.Cache.load_factor );
	}
	
	public HostCache( int hostId ) {
		this();
		this.hostId = hostId;
		key = NodeUtil.encoder.encodeToString( getBytes() );
	}
	
	public HostCache( String key ) {
		this(  NodeUtil.decoder.decode( key ) );
		this.key = key;		
	}
	
	public HostCache( byte b[] ) {
		this();
		ByteBuffer wrapped = ByteBuffer.wrap( b );
		hostId = wrapped.getInt();
	}

	
	public String getKey() {
		return key;
	}

	public byte[] getBytes() {
		return  ByteBuffer.allocate( HostCache.BYTES ).putInt( hostId ).array();
	}	

	
	public void pruneNodes() {
		LOG.info( "pruning hostId: " + hostId + " with size " + nodes.size() );
		
		synchronized( nodes ) {
			Iterator< Map.Entry< String, NodeId > > itr = nodes.entrySet().iterator();			
					
			while( itr.hasNext() ) {
				Map.Entry< String, NodeId > entry = itr.next();
				NodeId node = entry.getValue();
				
				if( Settings.FThreshold.collect < node.paths.size() ) {
					LOG.info( "removing node: " + entry.getKey() + " with size:" + node.paths.size() + " num of saved paths:" + node.numSavedPath );
			
					itr.remove();
				}
			}
			needPrune = false;
		}
		
		LOG.info( "size after pruning: " + nodes.size() );
	}


	
	@Override
	public int hashCode() {
		return hostId; // ^ pageNodeId.hashCode();
		//return key.hashCode();
	}

	@Override
	public boolean equals( Object obj ) {
		HostCache rhs = ( HostCache ) obj;
		return hostId == rhs.hostId && key.equals( rhs.key );
	}

	@Override
	public String toString() {
		return "host:" + hostId + " key:" + key + " number of noes:" + nodes.size();
	}
	
	// test
	static public void main( String args[] ) {
		HostCache id = new HostCache( "MjM0NTQ2" );
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
