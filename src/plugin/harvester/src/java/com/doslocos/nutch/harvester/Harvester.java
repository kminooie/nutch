package com.doslocos.nutch.harvester;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

// import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
// import org.jsoup.select.Elements;        
import org.jsoup.nodes.Node;

import com.doslocos.nutch.harvester.storage.Storage;
import com.doslocos.nutch.util.NodeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Harvester {

	static public final Logger LOG = LoggerFactory.getLogger( Harvester.class );

	private int frequency_threshould ;

	private String connClassName;
	private Class<?> connClass;
	

	public Harvester( Configuration conf ){

		frequency_threshould =  conf.getInt( "doslocos.harvester.frequency_threshould" , 2  );
		LOG.info( "frequency threshould: " + frequency_threshould );

		NodeUtil.selectList = conf.get( "doslocos.harvester.selector", NodeUtil.selectList );
		LOG.info( "selectList: " + NodeUtil.selectList );

		connClassName = conf.get( "doslocos.harvester.storage.class", null );
		if( null == connClassName ) {
			LOG.error( "storage class ( doslocos.harvester.storage.class ) is not set. this property is mandatory." );
			LOG.error( "available options are:" );
			LOG.error( "com.doslocos.nutch.harvester.ConnectMysql" );
			LOG.error( "com.doslocos.nutch.harvester.ConnectRedis" );

			die();
		}
		LOG.info( "storage class: " + connClassName );

		try {
			connClass = Class.forName( connClassName );

			Method set = connClass.getMethod( "set", Configuration.class );
			set.invoke( null, conf );

		} catch( Exception e ) {
			LOG.error( "Got exception while setting storage class: ", e );
			die();
		}
	}

	
	public void die() {
		System.exit( 1 );
	}

	
	public boolean learn( String HTMLBody, String host, String path ) {
		
		Map<PageNodeId, Integer > map = null;
		boolean result = false;
		
		Storage storage = getStorage( host, path );

		try{
			Node pageNode = NodeUtil.parseDom( HTMLBody );

			readAllNodes( storage, pageNode, "html/body" );			
			map = storage.getAllFreq();

			updateNodes( storage, map, pageNode, "html/body" );
			
			storage.pageEnd();

			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsing host: " + host + " path: " + path, e );
		}

		LOG.debug( "number of db roundtrip while learning:" + storage.counter +"  "+host+path);

		return result;
	}


	public String filter( String HTMLBody, String host ) {

		Map<PageNodeId, Integer > map = null;
		String result = null;
		
		Storage storage = getStorage( host, null );

		try{
			Node pageNode = NodeUtil.parseDom( HTMLBody );
			
			readAllNodes( storage, pageNode, "html/body" );			
			map = storage.getAllFreq();

			result = filterNode( storage, map, pageNode, "html/body" );

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
		
		// if( 32 == hash ) return;
		
		k.addNodeToList( xpath.hashCode(), hash );
		for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
			readAllNodes( k, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
		}
	}

	
	
	private void updateNodes( final Storage storage, final Map<PageNodeId,Integer> map, final Node node, final String xpath ) {
		PageNodeId item = new PageNodeId( xpath.hashCode(), node.hashCode() );
		Integer fq = map.get( item );
		
		if( null == fq ||  fq < 1 ) {
			// TODO fix this
			// storage.addNode( )
			// no adding, cache threshold wouldn't apply
			
			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				updateNodes( storage, map, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
			}
		}
	}


	
	private String filterNode( final Storage storage, final Map<PageNodeId,Integer> map, final Node node, final String xpath ) {
		int hash = node.hashCode();
		String content = "";

		// if( 32 == hash ) return content;

		int freq = map.get( new PageNodeId( xpath, hash ) );
		
		if( freq < frequency_threshould ) {
			content = NodeUtil.extractText( node );

			for( int i = 0, size = node.childNodeSize(); i < size; ++i ){
				Node child = node.childNode( i );
				String newXpath = xpath + "/" + NodeUtil.xpathMaker( child );
				content = content + " " + filterNode( storage, map, child, newXpath );
			}		
		}

		return content.trim();
	}


}
