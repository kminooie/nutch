/**
 * Utility class for manipulating node
 *
 */

package com.doslocos.nutch.util;

import java.util.Base64;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;


public class NodeUtil {

	static public final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
	static public final Base64.Decoder decoder = Base64.getDecoder();
	
	public static String removeList =
		"server,appserver,meta,link,timestamp,noscript,script,style,form,option,input,select,button,comment,#comment,#text,.hidden"
	;
	
	public static Node parseDom( String page_content ) {
		Document doc = Jsoup.parse( page_content );
		doc.select( removeList ).remove();
		Elements e = doc.getElementsByTag( "body" );
		return e.get( 0 );
	}


	public static String xpathMaker( Node node ) {

		int fre = 1;
		Node ft = node;
		for( int count = node.siblingIndex(); count > 0; --count ) {
			ft = ft.previousSibling();
			if( ft.nodeName() == node.nodeName() ) {
				++fre;
			}		
		}

		if(node.nodeName().startsWith("#")){
			return(node.nodeName().substring(1)+"()"+"["+fre+"]");
		}else{
			return(node.nodeName()+"["+fre+"]");
		}

	}


	public static String extractText( Node node ) {
		String string1="";
		if (node.hasAttr("text")){
			string1=node.attr("text").replaceAll("\\s+", " ").trim();
		}

		return string1;	
	}


	static public int stringToId( String str ) {
		return str.hashCode();
	}

}