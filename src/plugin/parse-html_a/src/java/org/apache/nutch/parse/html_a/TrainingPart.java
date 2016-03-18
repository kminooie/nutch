package org.apache.nutch.parse.html_a;


import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.protocol.Content;
import org.jsoup.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



//public class TrainingPart implements IndexingFilter{

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
	public static String HTMLBody;
	public TrainingPart() {

	}

	public ParseResult getParse(Content content) {
		
		LOG.info("kaveh, the training part class called");

		//to extract the content of a page

		String HTMLBody = new String(content.getContent());
		//step 1     String fieldName = "rawcontent";
		String rawcontent=HTMLBody;
		node = kbc.parseDom( HTMLBody );

		try {
			netUrl = new URL(content.getUrl());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		kbc.makeDatabase( node, "html/body", netUrl.getHost(), netUrl.getPath() );
		LOG.info("kaveh, the page add to database with url :  "+netUrl.getPath());
		
		// collect outlink
		Outlink[] outlinks = OutlinkExtractor.getOutlinks( HTMLBody, getConf());
		
	//step1	ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, fieldName,
		//step1		outlinks, content.getMetadata());
		//step1	return ParseResult.createParseResult(content.getUrl(), new ParseImpl( HTMLBody,
		//step1		parseData));		
		
		/*	
		ParseResult parseResult = new ParseResult(content.getUrl());
		ParseText parseText = new ParseText(HTMLBody);
		ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
				outlinks, content.getMetadata());				
		*/
		return ParseResult.createParseResult(content.getUrl(), new ParseImpl(HTMLBody,
				new ParseData(ParseStatus.STATUS_SUCCESS, rawcontent, outlinks, 
                content.getMetadata())));
		
		
		
		
		
		
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
