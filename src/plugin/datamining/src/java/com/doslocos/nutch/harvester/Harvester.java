package com.doslocos.nutch.harvester;

import com.doslocos.nutch.util.NodeUtil;

import org.apache.hadoop.conf.Configuration;
// import org.jsoup.Jsoup;
// import org.jsoup.nodes.Document;
// import org.jsoup.select.Elements;        
import org.jsoup.nodes.Node;

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

		util.selectList = conf.get( "doslocos.harvester.selector", util.selectList );
		LOG.info( "selectList: " + util.selectList );

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

			Method set = lc.getMethod( "set" );
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
		Storage k = null;

		try {
			Class<?> implClass = Class.forName( connClassName );
			k = (Storage) implClass.getConstructor( Configuration.class ).newInstance( conf );
		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		k.counter = 0;


		try{
			Node pageNode;
			pageNode = parseDom( HTMLBody );

			hostId = k.getHostId( host );
			pathId = k.getPathId( hostId, path );

			readNode( k, pageNode, "html/body", pathId, hostId );




			k.emptyBatch(pathId);






			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsingFunction " + hostId + "   " + pathId, e );
		}

		LOG.debug( "number of db roundtrip while learning:" + k.counter +"  "+host+path);

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

		Node nodePage = parseDom( rawcontent );

		String result = checkNode( k, nodePage, "html/body", hostId );

		LOG.debug( "number of db roundtrip while filtering: " + k.counter + " ,url : "+ host);

		return result;
	}



	private void readNode( Storage k, Node node, String xpath, int pathId, int hostId ) {
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
				readNode( k, node.childNode( i ), xpath+"/"+xpathMaker( node.childNode( i ) ), pathId, hostId );
			}
		}

	}

	private String checkNode( Storage k, Node node, String xpath, int hostId ) {
		int hash = node.toString().hashCode();
		String content = "";

		if( 32 == hash ) return content;

		int freq = k.getNodeFreq( hostId, hash, xpath );
		if( freq < frequency_threshould ) {
			content = extractText( node );

			for( int i = 0, size = node.childNodeSize(); i < size; ++i ){
				String newXpath = xpath + "/" + xpathMaker( node.childNode( i ) );
				content = content + " " + checkNode( k, node.childNode( i ), newXpath, hostId );
			}		
		}

		return content.trim();
	}

}
