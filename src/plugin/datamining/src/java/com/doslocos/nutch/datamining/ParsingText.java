package com.doslocos.nutch.datamining;

import org.apache.hadoop.conf.Configuration;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsingText {

	private int frequency_threshould ;
	private String selector;
	private int hostId, pathId ;
	
	private String connClassName ;
	private Configuration conf ;
	public static final Logger LOG = LoggerFactory.getLogger(ParsingText.class);


	//constructor of class
	public ParsingText(Configuration conf){

		frequency_threshould = Integer.parseInt( conf.get( "doslocos.training.frequency_threshould" , "com.doslocos.nutch.datamining.ConnectRedis") );
		selector = conf.get(
				"doslocos.training.selector",
				"script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden"
				);

		// conn1 = new ConnectMysql( conf );
		//	 conn1 = new ConnectRedis( conf );
		this.conf = conf;
		connClassName = conf.get( "doslocos.training.storage.class" );
		
		LOG.debug( "Using: " + connClassName + " for storage" );

	}


	public boolean learn( String HTMLBody, String host, String path ) {
		boolean result = false;
		Knowledge k = null;
		
		try {
			Class<?> implClass = Class.forName( connClassName );
			// (Knowledge) implClass.newInstance();
			k = (Knowledge) implClass.getConstructor( Configuration.class ).newInstance( conf );
		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		k.counter = 0;
		
		
		try{
			Node pageNode;
			pageNode = parseDom( HTMLBody );
			
			hostId = k.getHostId( host );
			pathId = k.getPathId( hostId, path );
			//			hostId=host.hashCode();
			//			pathId=path.hashCode();


			readNode( k, pageNode, "html/body", pathId, hostId );

			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsingFunction " + hostId + "   " + pathId, e );
		}

		LOG.debug( "number of db roundtrip while learning:" + k.counter +"  "+host+path);

		return result;

	}


	public String filter( String rawcontent, String host ) {
		Knowledge k = null;
		
		try {
			Class<?> implClass = Class.forName( connClassName );
			// (Knowledge) implClass.newInstance();
			k = (Knowledge) implClass.getConstructor( Configuration.class ).newInstance( conf );
		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		k.counter = 0;
		
		//int hostId = conn1.getHostId( host );
		hostId=host.hashCode();

		Node nodePage = parseDom( rawcontent );

		//compare web page with database
		String result = checkNode( k, nodePage, "html/body", hostId );
		LOG.debug( "number of db roundtrip while filtering: " + k.counter + " ,url : "+ host);
		return result;
	}

	//change a string to a DOM and return a node
	private Node parseDom( String page_content ) {
		Document doc = Jsoup.parse( page_content );
		doc.select( selector ).remove();
		Elements ele = doc.getElementsByTag( "body" );
		Node node1 = ele.get( 0 );
		return node1;

	}

	//this function make an xpath for the nodes  done!
	private static String xpathMaker( Node node ) {

		int fre=1;
		Node ft=node;
		for(int count=node.siblingIndex();count>0;count--){
			ft=ft.previousSibling();
			if(ft.nodeName()==node.nodeName()){
				fre++;
			}		
		}

		if(node.nodeName().startsWith("#")){
			return(node.nodeName().substring(1)+"()"+"["+fre+"]");
		}else{
			return(node.nodeName()+"["+fre+"]");
		}

	}


	//this function extract the text exist in each nodes 
	private static String extractText( Node node ) {
		String string1="";
		if (node.hasAttr("text")){
			string1=node.attr("text").replaceAll("\\s+", " ").trim();
		}

		return string1;	
	}


	private void readNode( Knowledge k, Node node, String xpath, int pathId, int hostId ) {
		int hash = node.toString().hashCode();
		boolean nodeExist = true;
		// empty node, ignored
		if( 32 == hash ) return;

		try{
			nodeExist = k.addNode( hostId, pathId, hash, xpath );

		}catch(Exception e){

			LOG.debug("Error happened during add a node :"+xpath +"   "+hostId+"   "+ pathId+"   "+ hash, e);
		}

		if( nodeExist && node.childNodeSize() > 0 ) {

			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				readNode( k, node.childNode( i ), xpath+"/"+xpathMaker( node.childNode( i ) ), pathId, hostId );
			}
		}

	}

	private String checkNode( Knowledge k, Node node, String xpath, int hostId ) {
		int hash = node.toString().hashCode();
		String content = "";

		// empty node, ignored
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
