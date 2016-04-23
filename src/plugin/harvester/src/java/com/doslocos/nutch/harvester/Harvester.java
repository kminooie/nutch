package com.doslocos.nutch.harvester;


import org.apache.hadoop.conf.Configuration;
// import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
// import org.jsoup.select.Elements;        
import org.jsoup.nodes.Node;
import java.lang.reflect.Method;

import com.doslocos.nutch.util.NodeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Harvester {

	private static int frequency_threshould ;
	private int hostId, pathId ;

	private String connClassName;
	private Class<?> connClass;
	private Configuration conf ;
	public static final Logger LOG = LoggerFactory.getLogger( Harvester.class );


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

			Method set = connClass.getMethod( "set" );
			set.invoke( null, conf );

		} catch( Exception e ) {
			LOG.error( "Got exception while setting storage class: ", e );
			die();
		}

		this.conf = conf;		
	}

	public void die() {
		System.exit( 1 );
	}

	public boolean learn( String HTMLBody, String host, String path ) {
		boolean result = false;
		Storage storage = null;
		Map<NodeItem, Integer > map = null;
		try {			
			storage = (Storage) connClass.getConstructor( String.class, String.class ).newInstance( host, path );
		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		storage.counter = 0;


		try{
			Node pageNode;
			pageNode = NodeUtil.parseDom( HTMLBody );

			readAllNodes( storage, node , xpath );			
			map = storage.getAllFreq();

			updateNodes( storage, map, node, xpath );
			
			storage.pageEnd();

			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsing host: " + host + " path: " + path, e );
		}

		LOG.debug( "number of db roundtrip while learning:" + storage.counter +"  "+host+path);

		return result;
	}


	public String filter( String rawcontent, String host ) {

		Storage k = null;

		try {
			Class<?> implClass = Class.forName( connClassName );
			k = (Storage) implClass.getConstructor( Configuration.class ).newInstance( conf );
		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		k.counter = 0;

		hostId=host.hashCode();

		Node nodePage = NodeUtil.parseDom( rawcontent );

		String result = checkNode( k, nodePage, "html/body", hostId );

		LOG.debug( "number of db roundtrip while filtering: " + k.counter + " ,url : "+ host);

		return result;
	}


	public void readAllNodes( Storage k, Node node, String xpath ) {
		Integer hash = node.hashCode();
		if( 32 == hash ) return;
		k.addNodeToList( xpath.hashCode(), hash );
		for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
			readAllNodes( k, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
		}
	}

	
	public void updateNodes( final Storage storage, final Map<NodeItem,Integer> map, final Node node, final String xpath ) {
		NodeItem item = New NodeItem( xpath.hashCode(), node.hashCode() );
		Integer fq = map.get( NodeItem );
		
		if( null = fq ||  fq < 1 ) {
			// TODO fix this
			// storage.addNode( )
			
			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				updateNodes( storage, map, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
			}
		}
	}
	
	@deprecated
	private void readNode( Storage k, Node node, String xpath ) {
		


		int hash = node.toString().hashCode();
		boolean nodeExist = true;

		if( 32 == hash ) return;

		try{
			nodeExist = k.addNode( hostId, pathId, hash, xpath );


		}catch(Exception e){

			LOG.debug("Error happened during add a node :"+xpath +"   "+hostId+"   "+ pathId+"   "+ hash, e);
		}

		if( nodeExist && (node.childNodeSize() > 0 )) {

			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				readNode( k, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ), pathId, hostId );
			}
		}

	}


	private String checkNode( Storage k, Node node, String xpath, int hostId ) {
		int hash = node.hashCode();
		String content = "";

		if( 32 == hash ) return content;

		int freq = k.getNodeFreq( hostId, hash, xpath );
		if( freq < frequency_threshould ) {
			content = NodeUtil.extractText( node );

			for( int i = 0, size = node.childNodeSize(); i < size; ++i ){
				String newXpath = xpath + "/" + NodeUtil.xpathMaker( node.childNode( i ) );
				content = content + " " + checkNode( k, node.childNode( i ), newXpath, hostId );
			}		
		}

		return content.trim();
	}

}
