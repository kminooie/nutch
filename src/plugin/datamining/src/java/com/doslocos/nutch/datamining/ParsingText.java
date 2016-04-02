package com.doslocos.nutch.datamining;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsingText {
	public static String Schema_2locos_tariningpart;
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static String Host_2locos_tariningpart;
	public static int frequency_threshould ;

	public static Knowledge conn1;

	public static final Logger LOG = LoggerFactory.getLogger(ParsingText.class);


	//constructor of class
	public ParsingText(String schema2locos,String host2locos,String pass2locos,String user2locos){


		conn1=new ConnectMysql2(schema2locos,host2locos,pass2locos,user2locos);


	}
	
	//add url and host to database and extract the urlID from it
	public long addUrlHost(String hostName, String pathName){
		
		long urlID=0;
		
		urlID=conn1.addUrlHostDb(hostName, pathName);
				
		return urlID;
	}
	
	

	//this function traverses a node and put them in database
	public void makeDatabase(Node node,String xpath,String host, String path,long tempUrlId) {
		//put the node in database

		try{
			boolean nodeExist=conn1.addIncNode(host,path, xpath, node.toString(),tempUrlId);

			if (node.childNodeSize()> 0&& nodeExist){

				for (int childrennum=0;childrennum<node.childNodeSize();childrennum++){
					makeDatabase(node.childNode(childrennum),xpath+"/"+xpathMaker(node.childNode(childrennum)),host,path,tempUrlId);
				}

			}
		}catch(Exception e){
			LOG.error("Error happened during calling the children :"+xpath);

		}

	}



	/*the old one
	//this function traverses a node and put them in database
	public void makeDatabase(Node node,String xpath,String host, String path) {
		//put the node in database
		try{
			conn1.addIncNode(host,path, xpath, node.toString());
		} catch (Exception e) {
			LOG.error("Error happened during adding a node to database: "+xpath);
		}
		try{
			if (node.childNodeSize()>0){

				for (int childrennum=0;childrennum<node.childNodeSize();childrennum++){
					makeDatabase(node.childNode(childrennum),xpath+"/"+xpathMaker(node.childNode(childrennum)),host,path);
				}

			}
		}catch(Exception e){
			LOG.error("Error happened during calling the children :"+xpath);

		}

	}
	 */


	//this function make an xpath for the nodes  done!
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


	//change a string to a DOM and return a node
	public Node parseDom (String page_content){
		Document doc = Jsoup.parse(page_content);
		doc.select("script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden").remove();
		Elements ele=doc.getElementsByTag("body");
		Node node1=ele.get(0);
		return node1;

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

