package com.doslocos.nutch.harvester;

import java.lang.reflect.Method;

import org.jsoup.nodes.Node;

import com.doslocos.nutch.harvester.storage.Storage;
import com.doslocos.nutch.util.NodeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Harvester {

	static public final Logger LOG = LoggerFactory.getLogger( Harvester.class );
	
	static private Class<?> connClass;


	public synchronized void init() {
		if( null == connClass ) {
			try {
				connClass = Class.forName( Settings.Storage.connClassName );

				Method init = connClass.getMethod( "init" );
				init.invoke( null  );

			} catch( Exception e ) {
				LOG.error( "Got exception while calling init: ", e );
				// die();
			}

		} else {
			LOG.warn( "Harvester is already initialized." );
		}
	}
	
	public boolean learn( String HTMLBody, String host, String path ) {
		LOG.debug( "start learning host: " + host + " path: " + path );
		boolean result = false;
		Node pageNode = null;
		Storage storage = getStorage( host, path );

		try{
			pageNode = NodeUtil.parseDom( HTMLBody );

			updateNodes( storage, pageNode, "html/body" );

			storage.learnEnd();
			
			result=true;
		}catch( Exception e ){
			LOG.error( "Exception while parsing host: " + host + " path: " + path, e );
		}

		return result;
	}


	public String filter( String HTMLBody, String host, String path ) {
		LOG.debug( "start filtering host: " + host + " path: " + path );
		
		String result = null;

		Storage storage = getStorage( host, path );

		try{
			Node pageNode = NodeUtil.parseDom( HTMLBody );

			result = filterNode( storage, pageNode, "html/body" );
			
			storage.filterEnd();

			LOG.debug( "filter function finished for: " + host + " path: " + path );
		}catch( Exception e ){
			LOG.error( "Exception while filtering host: " + host + " path: " + path, e );
		}

		return result;
	}


	private void updateNodes( final Storage storage, final Node node, final String xpath ) {
		int frequency = storage.addNodeToCurrentHost( xpath, NodeUtil.getNodeHashCode( node ) );
		if( frequency <= Settings.Frequency.collect ) {

			for (int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				updateNodes( storage, node.childNode( i ), xpath+"/"+NodeUtil.xpathMaker( node.childNode( i ) ) );
			}
		}
	}


	private String filterNode( final Storage storage, final Node node, final String xpath ) {
		int frequency = storage.getNodeFrequency( xpath, NodeUtil.getNodeHashCode( node ) );
		String content = "";

		if( frequency <= Settings.Frequency.collect ) {
			content = NodeUtil.extractText( node );

			for( int i = 0, size = node.childNodeSize(); i < size; ++i ) {
				Node child = node.childNode( i );
				String newXpath = xpath + "/" + NodeUtil.xpathMaker( child );
				content += " " + filterNode( storage, child, newXpath );
			}		
		} else {
			LOG.debug( "dropping node with hash:" + NodeUtil.getNodeHashCode( node ) + " with freq:" + frequency );
		}

		return content.trim();
	}


	private Storage getStorage( String host, String path ) {
		Storage storage = null;

		try {			
			storage = (Storage) connClass.getConstructor( String.class, String.class ).newInstance( host, path );
		} catch( Exception e ) {
			LOG.error( "Failed to instanciate storage: ", e );
		}

		return storage;
	}


}
