package org.apache.nutch.indexwriter.solr;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsingPart {


	public static final Logger LOG = LoggerFactory.getLogger(ParsingPart.class);	

	public static ConnectionKB conn1;

	public static int frequency_threshould ;


	public ParsingPart(){
		LOG.info("kaveh, the parsingPart class created.");
		conn1=new ConnectMysql();
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
		LOG.info("kaveh, the node that recieved by compareKB is : "+node_1.toString());
		freq = conn1.getNodeFreq( domain, xpath, node_1.toString() );

		if ( freq > frequency_threshould){
			//assign value 1 to a node whom exists in database

			LOG.info("alirezaa kaveh, node exist in KB and frequency is : "+freq+"  of node is more than threshold. the xpath is : "+xpath +"and the domain is: "+domain+ " and the content of node is : "+node_1.toString());


		} else {
			LOG.info("alirezaa kaveh,this node frequency is lower than threshold in KB "+xpath +"the content of node is : "+node_1.toString());
			//check how many children the node has
			content=extractText(node_1);
			if (node_1.childNodeSize() > 0 ) {
				//	LOG.info("kaveh, the node has children so call compareKb for all the children");
				LOG.info("kaveh, the children size is: "+node_1.childNodeSize()+" of node with xpath:  "+xpath+" and with content : "+node_1.toString() );

				for (int i1=0;i1<node_1.childNodeSize();i1++){
					LOG.info("kaveh, children called is : "+node_1.childNode(i1).toString());
					String newXpath=xpath+"/"+xpathMaker(node_1.childNode(i1));
					content=content+" "+compareKB(node_1.childNode(i1),newXpath,domain);
					LOG.info( "kaveh, compareKB called for children of node with path: "+newXpath);

				}

			}
		}
		return content;

	}


	/*
	//the different between comapreKB_new and old version is in new version the value tree created in comparing part not befor that and the 
	//size and number of nodes is different with original page bc its not created any nodes for duplicate nodes
	public void compareKB_new(Node node_1,NodeTree node_2, String xpath, String domain){

		LOG.info("kaveh, compareKB function called");
		int freq = 0;

		freq = conn1.getNodeFreq( domain, xpath, node_1.toString() );

		if ( freq > frequency_threshould ){
			//assign value 1 to a node whom exists in database
			node_2.addValue(1);
			LOG.info("kaveh, node exist in KB and frequency of node is more than threshold");
		} else {
			LOG.info("kaveh, value assign as 0");
			node_2.addValue(0);

			//check how many children the node has
			if (node_1.childNodeSize() > 0 ) {
				LOG.info("kaveh, the node has children so call compareKb for all the children");

				//there are children so traverse the node
				for (Node nextNode:node_1.childNodes()){

					compareKB_new(nextNode,node_2.addChild(xpath+"/"+xpathMaker(nextNode)) ,xpath+"/"+xpathMaker(nextNode),domain);
					LOG.info( "kaveh, compareKB called for children of node with path:"+xpath+"/"+xpathMaker(nextNode) );
				}
			}
		}

		LOG.info( "kaveh, compareKB finished for node with:" + xpath );

	}

	//to calculate the valuetree recursively
	public double calculateValueTree(NodeTree valuenode){
		double result=0;

		if(valuenode.valueNode()==1){
			result=1;
		}else{
			if (valuenode.childrenSize()>0){
				double tempresult=0;
				for (NodeTree newnode:valuenode.children()){
					tempresult=tempresult+calculateValueTree(newnode);
				}
				tempresult=tempresult/valuenode.childrenSize();


				result=tempresult;
			}else{
				result=valuenode.valueNode();
			}
		}
		return result;
	}


	//this method is a private method to extract all the text exist in DOM
	//a node and a empty string as input and a string contains all the text as an output
	public static String Parsingpage(Node node,String sb){
		String string1="";
		if (node.hasAttr("text")){
			string1=string1+" "+node.attr("text").replaceAll("\\s+", " ");
		}

		if(node.childNodeSize()>0){
			for (Node sam:node.childNodes()){
				string1=string1+Parsingpage(sam, string1);
			}

		}	
		return string1;	
	}

	 */
	//this function extract the text exist in each nodes 
	public static String extractText(Node node){
		String string1="";
		if (node.hasAttr("text")){
			string1=node.attr("text").replaceAll("\\s+", " ");
		}

		return string1;	
	}



}
