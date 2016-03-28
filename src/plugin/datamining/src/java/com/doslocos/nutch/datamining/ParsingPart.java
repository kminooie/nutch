package com.doslocos.nutch.datamining;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsingPart {


	public static final Logger LOG = LoggerFactory.getLogger(ParsingPart.class);	

	public static ConnectMysql conn1;

	public static int frequency_threshould ;


	public ParsingPart(){
		LOG.info("kaveh, the parsingPart class from IndexingPart  created.");
		conn1=new ConnectMysql(IndexingPart.Schema_2locos_tariningpart,IndexingPart.Host_2locos_tariningpart,IndexingPart.PASS_2locos_tariningpart,IndexingPart.USER_2locos_tariningpart);
	}


	//parseDom used for pre-processing on a web page to prepare it for next step
	public Node parseDom (String page_content){
		Document doc = Jsoup.parse(page_content);
		doc.select("script,style,option,input,form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden").remove();
		Elements ele=doc.getElementsByTag("body");
		Node node1=ele.get(0);
		LOG.info("kaveh, a node parsed with parseDom (changed a string to DOM model base)");
		return node1;

	}


	//pathMaker used to produce xpath for each node
	public static String xpathMaker(Node node){

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



	//use compareKB_new
	public static String compareKB(Node node_1, String xpath, String domain){
		String content="";
		int freq = 0;
		freq = conn1.getNodeFreq( domain, xpath, node_1.toString() );

		if ( freq > frequency_threshould){
			//assign value 1 to a node whom exists in database
			//Do not nothing because the frequency more than threshold and you must skip it


		} else {
			content=extractText(node_1);
			if (node_1.childNodeSize() > 0 ) {

				for (int i1=0;i1<node_1.childNodeSize();i1++){
					String newXpath=xpath+"/"+xpathMaker(node_1.childNode(i1));
					content=content+" "+compareKB(node_1.childNode(i1),newXpath,domain);

				}

			}
		}
		return content.trim();

	}



	//this function extract the text exist in each nodes 
	public static String extractText(Node node){
		String string1="";
		if (node.hasAttr("text")){
			string1=node.attr("text").replaceAll("\\s+", " ").trim();
		}

		return string1;	
	}



}
