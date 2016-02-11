package org.apache.nutch.parse.html_a;

import java.sql.SQLException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

public class KnowledgeBaseCompare {

	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/KnowledgeBase";

	// Database credentials
	static final String USER = "pasha";
	static final String PASS = "2014";
	static ConnectMysql conndb;

	//change a string to a DOM and return a node
	public Node parseDom (String page_content){

		Document doc = Jsoup.parse(page_content);
		doc.select("script,style,option,input, form,meta,input,select,appserver,button, comment,#comment,#text,noscript,server,timestamp,.hidden").remove();
		Elements ele=doc.getElementsByTag("body");
		Node node1=ele.get(0);
		return node1;

	}
	//this function traverses a node and put them in database
	public void makeDatabase(Node node,String xpath) throws SQLException{

		//put the node in database
		conndb.addIncNode("www.2locos.com","category", xpath, node.toString());

		if (node.childNodeSize()>1){

			for (int childrennum=0;childrennum<node.childNodeSize();childrennum++){
				makeDatabase(node.childNode(childrennum),xpath+"/"+xpathMaker(node.childNode(childrennum)));
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
	public KnowledgeBaseCompare() throws ClassNotFoundException, SQLException{
		conndb=new ConnectMysql(USER,PASS,JDBC_DRIVER,DB_URL);

	}

	//destructor to release the connection to database
	protected void finalize() throws SQLException{

		conndb.finalize();

	}



}
