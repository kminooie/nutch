package org.apache.nutch.parse.html_a;


import java.net.MalformedURLException;
import java.net.URL;

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
	public static String  schema_2locos_tariningpart;
	//the variable and constant 
	public static String domain_2locos_tariningpart = "localhost";

	// Database credentials
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static Configuration conf;

	public static KnowledgeBaseCompare kbc;
	public static Node node;
	public static URL netUrl;
	public TrainingPart() {

	}

	public ParseResult getParse(Content content) {


		//to extract the content of a page
		String baseUrl=content.getBaseUrl();
		String HTMLBody=content.getContent().toString();

		LOG.info("the web page with :"+baseUrl +"parsed with TrainingPart code.");

		node=kbc.parseDom( HTMLBody );

		try {
			netUrl = new URL(content.getUrl());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		kbc.makeDatabase(node, "html/body",netUrl.getHost(),netUrl.getPath());

		ParseResult result = new ParseStatus(ParseStatus.SUCCESS, "Parsed." ).getEmptyParseResult( content.getUrl(), getConf() );
		Parse parse = result.get(content.getUrl());
	    Metadata metadata = parse.getData().getParseMeta();
	    metadata.add( "rawcontent", HTMLBody );
	    
	    return result;
	}


	public void setConf(Configuration conf) {
		
		schema_2locos_tariningpart=conf.get("database.schema");
		USER_2locos_tariningpart=conf.get("database.username");
		PASS_2locos_tariningpart=conf.get("database.password");
		
		kbc = new KnowledgeBaseCompare();
		TrainingPart.conf = conf;
	}

	public Configuration getConf() {

		return TrainingPart.conf;
	}


}
