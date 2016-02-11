package org.apache.nutch.parse.html_a;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.Content;
import org.jsoup.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.parse.html_a.KnowledgeBaseCompare;






public class TrainingPart {

	static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost/KnowledgeBase";

	// Database credentials
	static final String USER = "pasha";
	static final String PASS = "2014";


	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html_a");

	private Configuration conf;
	
	private static KnowledgeBaseCompare node1;
	
	
	public TrainingPart() {
		if( null == node1 ) {
			node1 = new KnowledgeBaseCompare();
		}
	}

	public void getParse(Content content) throws IOException ,ClassNotFoundException, SQLException{

		//to extract the content of a page
		String contentType = content.getContentType();
		String baseUrl=content.getBaseUrl();
		String text=content.getContent().toString();
		String ulr=content.getUrl();



		Node node=node1.parseDom(text);

		//ConnectMysql asa=new ConnectMysql(USER,PASS,JDBC_DRIVER,DB_URL);

		node1.makeDatabase(node, "html/body");




		//System.out.println( asa.addNode("www.2locos.com","article","html/body/div[2]","<body>...</body>"));

		//asa.addIncNode("www.3locos.com","shopping","html/body/div[2]","<body><div>...</div></body>");
		//call destructor to release the database connection
		//asa.finalize();		

	}


	public void setConf(Configuration conf) {
		this.conf = conf;

	}



	public Configuration getConf() {
		return this.conf;
	}


}
