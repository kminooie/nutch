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
	static public final String CONF_PREFIX = "doslocos.harvester.";

	//new stuff
	static public ConcurrentHashMap< Integer, LRUCache< String, NodeId> > mainCache;

	/**
	 * @var int ft_* various frequency thresholds
	 */
	static private int ft_collect, ft_write, ft_max, ft_gc ;
	
	/**
	 * @var int cache_* various cache tuning
	 */
	static private int cache_hosts_per_job ,cache_nodes_per_page;
	static private float cache_load_factor;
	
	static private String connClassName = null;
	static private Class<?> connClass;

	/**
	 * would contain all the nodes for this object page ( host + path )
	 */
	public final LinkedHashMap< NodeId, NodeValue> currentPage = new LinkedHashMap< NodeId, NodeValue>( 4000, .9f );

	static synchronized public void setConf( Configuration conf ) {
		if( null == connClassName ) {
			String prefix = CONF_PREFIX;

			// parser settings
			NodeUtil.removeList = conf.get( prefix + "remove_list", NodeUtil.removeList );
			LOG.info( "selectList: " + NodeUtil.removeList );


			// frequency settings
			prefix += "frequency.";
			
			ft_collect =  conf.getInt( prefix + "collect" , 2  );
			LOG.info( "collect frequency threshold: " + ft_collect );

			ft_write =  conf.getInt( prefix + "write" , ft_collect  );
			LOG.info( "write frequency threshold: " + ft_write );

			ft_max =  conf.getInt( prefix + "max" , ft_write  );
			LOG.info( "max frequency threshold: " + ft_max );

			ft_gc =  conf.getInt( prefix + "gc" , 10  );
			LOG.info( "gc frequency threshold: " + ft_gc );


			// cache settings
			prefix = CONF_PREFIX + "cache.";
			cache_nodes_per_page = conf.getInt( prefix + "hosts_per_job", 2048 );
			LOG.info( "cache_nodes_per_page: " + cache_hosts_per_job );
			
			cache_nodes_per_page = conf.getInt( prefix + "nodes_per_page", 4096 );
			LOG.info( "cache_nodes_per_page: " + cache_nodes_per_page );

			cache_load_factor = conf.getFloat( prefix + "load_factor", 0.95f );
			LOG.info( "cache_load_factor: " + cache_load_factor );

			mainCache = new ConcurrentHashMap< Integer, LRUCache< String, NodeId > >( cache_hosts_per_job );

			// storage settings
			if( null != ( connClassName = conf.get( CONF_PREFIX + "storage.class", null ) ) ) {
				try {
					LOG.info( "storage class: " + connClassName );
					connClass = Class.forName( connClassName );

					Method setConf = connClass.getMethod( "setConf", Configuration.class );
					setConf.invoke( null, conf );

				} catch( Exception e ) {
					LOG.error( "Got exception while setting storage class: ", e );
					die();
				}
			} else {
				LOG.error( "storage class ( " + CONF_PREFIX + "storage.class ) is not set. this property is mandatory." );
				LOG.error( "available options are:" );
				LOG.error( "com.doslocos.nutch.harvester.storage.Mariadb" );
				LOG.error( "com.doslocos.nutch.harvester.storage.Redis" );

				die();
			}
		} else {
			LOG.error( "should not be here" );
		}
	}
	
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

		try{
			pageNode = NodeUtil.parseDom( HTMLBody );
		} catch( Exception e ) {
			LOG.error( "while parsing got:", e );
			return result;
		}

		try {
			Storage storage = getStorage( host, path );


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



	private void readAllNodes( Storage k, Node node, String xpath ) {
		Integer hash = node.hashCode();

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
		for( Map.Entry< Integer, LRUCache< String, NodeId > > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			LRUCache< String, NodeId > hostCache = entry.getValue();
			
			Storage.LOG.info( "hostId: " + hId );
			for( Iterator< Map.Entry< NodeId, Set< Integer > > > itr = hostCache.getAll().iterator(); itr.hasNext(); ) {
				Map.Entry< NodeId, Set< Integer > > pageEntry = itr.next();
				Storage.LOG.info( "node: " + pageEntry.getKey() + " size is:" + pageEntry.getValue().size() );
			}
		}
	}


	static public void pruneMainCache() {
		for( Map.Entry< Integer, LRUCache< NodeId, Set< Integer > > > entry: mainCache.entrySet() ) {
			Integer hId = entry.getKey();
			LRUCache< NodeId, Set< Integer > > hostCache = entry.getValue();
			
			Storage.LOG.info( "pruning hostId: " + hId + " with size " + hostCache.usedEntries() );
			
			// Iterator< Map.Entry< PageNodeId, Set< Integer > > > itr = hostCache.getAll().iterator();
			Iterator< Map.Entry< NodeId, Set< Integer > > > itr = hostCache.entrySet().iterator();			
			
			
			while( itr.hasNext() ) {
				Map.Entry< NodeId, Set< Integer > > pageEntry = itr.next();
				if( pageEntry.getValue().size() < ft_collect ) {
					Storage.LOG.info( "removing node: " + pageEntry.getKey() + " with size:" + pageEntry.getValue().size() );
					itr.remove();
				}				
				
			}
			
			Storage.LOG.info( "size after pruning: " + hostCache.usedEntries() );
		}
	}

}
