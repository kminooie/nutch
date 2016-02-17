package org.apache.nutch.parse.html_a;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import java.sql.SQLException;


public class KnowledgeBaseCompare {

	static Knowledge conndb;

	//change a string to a DOM and return a node
	public Node parseDom (String page_content){

		Document doc = Jsoup.parse(page_content);
		doc.select("script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden").remove();
		Elements ele=doc.getElementsByTag("body");
		Node node1=ele.get(0);
		return node1;

	}


	//this function traverses a node and put them in database
	public void makeDatabase(Node node,String xpath,String host, String path) {

		//put the node in database
		try{
			conndb.addIncNode(host,path, xpath, node.toString());
		} catch (SQLException e) {

		}
		if (node.childNodeSize()>1){

			for (int childrennum=0;childrennum<node.childNodeSize();childrennum++){
				makeDatabase(node.childNode(childrennum),xpath+"/"+xpathMaker(node.childNode(childrennum)),host,path);
			}

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
		conndb=new ConnectMysql();	
	}






}
