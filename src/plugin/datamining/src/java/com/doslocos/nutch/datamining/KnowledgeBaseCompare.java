package com.doslocos.nutch.datamining;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;


public class KnowledgeBaseCompare {

	static Knowledge conndb;
	public static final Logger LOG = LoggerFactory.getLogger(KnowledgeBaseCompare.class);

	//change a string to a DOM and return a node
	public Node parseDom (String page_content){
		Document doc = Jsoup.parse(page_content);
		doc.select("script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden").remove();
		Elements ele=doc.getElementsByTag("body");
		Node node1=ele.get(0);
		LOG.info("kaveh,the page changed to a node");
		return node1;

	}


	//this function traverses a node and put them in database
	public void makeDatabase(Node node,String xpath,String host, String path) {
		//put the node in database
		try{
			LOG.info("kaveh, the node with add to database "+xpath);
			conndb.addIncNode(host,path, xpath, node.toString());
		} catch (SQLException e) {
			LOG.info("alireza, some error happened during adding node to database: "+xpath);
		}
		try{
			if (node.childNodeSize()>0){

				for (int childrennum=0;childrennum<node.childNodeSize();childrennum++){
					makeDatabase(node.childNode(childrennum),xpath+"/"+xpathMaker(node.childNode(childrennum)),host,path);
				}

			}
		}catch(Exception e){
			LOG.info("alireza, some error happened for calling the children :"+xpath);

		}

	}



	//this function make an xpath for the nodes  done!
	public String xpathMaker(Node node){
		
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


	//constructor for make a connection with database
	public KnowledgeBaseCompare(){
		conndb=new ConnectMysql(TrainingPart.Schema_2locos_tariningpart,TrainingPart.Host_2locos_tariningpart,TrainingPart.PASS_2locos_tariningpart,TrainingPart.USER_2locos_tariningpart);
	}


}
