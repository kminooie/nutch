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
	private Knowledge conn1;
	private String selector;

	public static final Logger LOG = LoggerFactory.getLogger(ParsingText.class);


	//constructor of class
	public ParsingText(Configuration conf){
		frequency_threshould = Integer.parseInt( conf.get( "doslocos.training.frequency_threshould" ) );
		selector = conf.get(
			"doslocos.training.selector",
			"script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden"
		);
		
		String cn = conf.get( "doslocos.training.storage.class", "com.doslocos.nutch.datamining.ConnectRedis" );
        LOG.info( "Using: " + cn + " for storage" );
				
		// conn1 = new ConnectMysql( conf );
	// conn1 = new ConnectRedis( conf );

        try {
			 Class<?> implClass = Class.forName( cn );
        
			// (Knowledge) implClass.newInstance();
			conn1 = (Knowledge) implClass.getConstructor( Configuration.class ).newInstance( conf );

		} catch( Exception e ) {
			LOG.error( "Got exception while loading storage class: ", e );
		}
		
	}


	public boolean learn( String HTMLBody, String host, String path ) {
		boolean result = false;
		Knowledge.counter = 0;
		
		try{
			Node pageNode;
			pageNode = parseDom( HTMLBody );

			int hostId = conn1.getHostId( host );
			int pathId = conn1.getPathId( hostId, path );

			readNode( pageNode, "html/body", pathId, hostId );

			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsingFunction " + e );
		}

		LOG.info( "number of db roundtrip while learning:" + Knowledge.counter );
		
		return result;

	}


	public String filter( String rawcontent, String host ) {
		LOG.info("alirezaaa1");
		Knowledge.counter = 0;
		int hostId = conn1.getHostId( host );
		LOG.info("alirezaaa2"+hostId);

		Node nodePage = parseDom( rawcontent );
		LOG.info("alirezaaa3");

		//compare web page with database
		String result = checkNode(nodePage, "html/body", hostId );
		LOG.info( "number of db roundtrip while filtering:" + Knowledge.counter );
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


	private void readNode( Node node, String xpath, int pathId, int hostId ) {
		int hash = node.toString().hashCode();

		// empty node, ignored
		if( 32 == hash ) return;

		try{
			boolean nodeExist = conn1.addNode( hostId, pathId, hash, xpath );

			if( nodeExist && node.childNodeSize() > 0 ) {
				for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
					readNode( node.childNode( i ), xpath+"/"+xpathMaker( node.childNode( i ) ), pathId, hostId );
				}
			}
		}catch(Exception e){
			LOG.error("Error happened during calling the children :"+xpath +" ", e);
		}
	}

	private String checkNode( Node node, String xpath, int hostId ) {
		int hash = node.toString().hashCode();
		String content = "";

		// empty node, ignored
		if( 32 == hash ) return content;

		int freq = conn1.getNodeFreq( hostId, hash, xpath );
		
		if( freq < frequency_threshould ) {
			content = extractText( node );
			
			for( int i = 0, size = node.childNodeSize(); i < size; ++i ){
				String newXpath = xpath + "/" + xpathMaker( node.childNode( i ) );
				content = content + " " + checkNode( node.childNode( i ), newXpath, hostId );
			}		
		}
		
		return content.trim();
	}

}
