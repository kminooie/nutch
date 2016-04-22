/**
 * Utility class for manuplating node
 *
 */

package com.doslocos.nutch.util;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;


public class NodeUtil {

	public String selectList =
		"script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden"
	;
	
	public Node parseDom( String page_content ) {
		Document doc = Jsoup.parse( page_content );
		doc.select( selectList ).remove();
		Elements ele = doc.getElementsByTag( "body" );
		Node node1 = ele.get( 0 );
		return node1;

	}


	public static String xpathMaker( Node node ) {

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


	public static String extractText( Node node ) {
		String string1="";
		if (node.hasAttr("text")){
			string1=node.attr("text").replaceAll("\\s+", " ").trim();
		}

		return string1;	
	}

}