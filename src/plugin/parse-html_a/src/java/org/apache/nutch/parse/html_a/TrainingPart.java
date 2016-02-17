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
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.jsoup.nodes.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TrainingPart implements Parser{

	public static final Logger LOG = LoggerFactory.getLogger(TrainingPart.class);

	private Configuration conf;

	public static KnowledgeBaseCompare node1;
	public static Node node;

	public TrainingPart() {
		LOG.info( "aaaaaaaaaaaaaaaaaaaaaaaaaaa" );
		node1 = new KnowledgeBaseCompare();
	}

	public ParseResult getParse(Content content) {

		LOG.info("aaaaaaaaaaaaa" );
		//to extract the content of a page
		String baseUrl=content.getBaseUrl();
		String text=content.getContent().toString();
		String url=content.getUrl();

				
		node=node1.parseDom(text);

		node1.makeDatabase(node, "html/body",baseUrl,url);
		return null;

	}


	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	public Configuration getConf() {
		return this.conf;
	}


}
