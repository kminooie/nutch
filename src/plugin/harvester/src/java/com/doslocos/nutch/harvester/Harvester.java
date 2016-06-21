package com.doslocos.nutch.harvester;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

import org.jsoup.nodes.Node;

import com.doslocos.nutch.harvester.storage.Storage;
import com.doslocos.nutch.util.LRUCache;
import com.doslocos.nutch.util.NodeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Harvester {

	static public final Logger LOG = LoggerFactory.getLogger( Harvester.class );
	
	
	public Harvester() {
		
	}
	
	public Harvester( Configuration conf ){

		if( null == connClassName ) setConf( conf );		
	}


	static public void die() {
		// TODO: find out how to kill a mapreduce job
		System.exit( 1 );
	}


	public boolean learn( String HTMLBody, String host, String path ) {

		Map<NodeId, NodeValue > map = null;
		
		boolean result = false;
		Node pageNode = null;
		Storage storage = getStorage( host, path );
		
		HostCache hostCache = storage.loadHost( storage.hostId );

		try{
			pageNode = NodeUtil.parseDom( HTMLBody );
		} catch( Exception e ) {
			LOG.error( "while parsing got:", e );
			return result;
		}

		try {
			


			readAllNodes( storage, pageNode, "html/body" );			
			map = storage.getAllFreq();

			updateNodes( storage, map, pageNode, "html/body" );

			storage.pageEnd( true );

			LOG.debug("learning function finish for : "+ host+path);
			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsing host: " + host + " path: " + path, e );
		}

		return result;
	}


	public String filter( String HTMLBody, String host, String path ) {
		LOG.debug( "start filtering host: " + host + " path: " + path );
		Map<NodeId, NodeValue > map = null;
		String result = null;

		Storage storage = getStorage( host, path );

		try{
			Node pageNode = NodeUtil.parseDom( HTMLBody );

			readAllNodes( storage, pageNode, "html/body" );			
			map = storage.getAllFreq();

			result = filterNode( storage, map, pageNode, "html/body" );
			storage.pageEnd( false );

			LOG.debug("filter function finished for : "+host+path);
		}catch( Exception e ){
			LOG.error( "Exception while filtering host: " + host, e );
		}

		LOG.debug( "number of db roundtrip while filtering: " + storage.counter + " ,url : "+ host);

		return result;
	}


	private Storage getStorage( String host, String path ) {
		Storage storage = null;

		try {			
			storage = (Storage) connClass.getConstructor( String.class, String.class ).newInstance( host, path );
		} catch( Exception e ) {
			LOG.error( "Failed to instanciate storage: ", e );
			die();
		}

		return storage;
	}



	private void readAllNodes( Storage s, Node node, String xpath ) {
		Integer hash = node.hashCode();

		NodeId n = .nodes.get( NodeId.makeKey( xpath, hash ) );
		if( null == n ) {
			n = new NodeId( xpath, hash );
			n.addPath(pathId)
		}
		k.addNodeToList( xpath, hash );
		for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
			readAllNodes( k, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
		}
	}



	private void updateNodes( final Storage storage, final Map<NodeId, NodeValue> map, final Node node, final String xpath ) {
		NodeId item = new NodeId( xpath.hashCode(), node.hashCode() );

		NodeValue val = map.get( item );

		storage.incNodeFreq( item, val );

		if( null == val ||  val.frequency < ft_collect ) {

			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				updateNodes( storage, map, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
			}
		}
	}



	private String filterNode( final Storage storage, final Map<NodeId, NodeValue> map, final Node node, final String xpath ) {
		int hash = node.hashCode();
		String content = "";

		NodeId id = new NodeId( xpath, hash );
		NodeValue val  = map.get( id );

		//LOG.debug("filterNode: id:" + id + " val:" + val );
		if( null == val || val.frequency < ft_collect ) {
			content = NodeUtil.extractText( node );

			for( int i = 0, size = node.childNodeSize(); i < size; ++i ){
				Node child = node.childNode( i );
				String newXpath = xpath + "/" + NodeUtil.xpathMaker( child );
				content = content + " " + filterNode( storage, map, child, newXpath );
			}		
		}

		return content.trim();
	}


	protected void finalize() {
		System.err.println( "Harvester finalize was called" );
		LOG.info( "Harvester finalize was called." );
	}




	static public void dumpMainCache() {
		for( Map.Entry< Integer, HostCache > entry: Storage.mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			HostCache hostCache = entry.getValue();
			
			LOG.info( "hostId: " + hId );
			for( Iterator< Map.Entry< String, NodeId > > itr = hostCache.nodes.entrySet().iterator(); itr.hasNext(); ) {
				Map.Entry< String, NodeId > pageEntry = itr.next();
				LOG.info( "node: " + pageEntry.getKey() + " size is:" + pageEntry.getValue().paths.size() );
			}
		}
	}


	static public void pruneMainCache() {
		for( Map.Entry< Integer, HostCache > entry: Storage.mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			HostCache hostCache = entry.getValue();
			
			Storage.LOG.info( "pruning hostId: " + hId + " with size " + hostCache.nodes.size() );
			
			// Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.getAll().iterator();
			Iterator< Map.Entry< String, NodeId > > itr = hostCache.nodes.entrySet().iterator();			
			
			
			while( itr.hasNext() ) {
				Map.Entry< String, NodeId > pageEntry = itr.next();
				if( pageEntry.getValue().paths.size() < ft_collect ) {
					Storage.LOG.info( "removing node: " + pageEntry.getKey() + " with size:" + pageEntry.getValue().size() );
					itr.remove();
				}				
				
			}
			
			Storage.LOG.info( "size after pruning: " + hostCache.usedEntries() );
		}
	}

}
