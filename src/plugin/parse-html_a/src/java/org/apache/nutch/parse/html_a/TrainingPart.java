package org.apache.nutch.parse.html_a;


import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
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


		//to extract the content of a page
		//String baseUrl=content.getBaseUrl();
		//HTMLBody=content.getContent().toString();
		
		String HTMLBody = new String(content.getContent());
		
		node=kbc.parseDom( HTMLBody );

		try {
			netUrl = new URL(content.getUrl());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOG.info("HTMLBody is:"+HTMLBody);
		LOG.info("netUrl.getHost() is:"+netUrl.getHost());
		LOG.info("netUrl.getPath() is:"+netUrl.getPath());

		LOG.info("node is:"+node.toString());
		kbc.makeDatabase(node, "html/body",netUrl.getHost(),netUrl.getPath());

		ParseResult result = new ParseStatus(ParseStatus.SUCCESS, "Parsed." ).getEmptyParseResult( content.getUrl(), getConf() );
		//Parse parse = result.get(content.getUrl());
	   // Metadata metadata = parse.getData().getParseMeta();
	   // metadata.add( "rawcontent", HTMLBody );
	    
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
	/*
	
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
			throws IndexingException {
		
		//doc.add("rawHTMLBody",HTMLBody);
		LOG.info("kaveh, the doc is: "+doc.toString());
		LOG.info("kaveh, url.toString() is : "+url.toString());
		LOG.info("kaveh, datum.toString() is: "+datum.toString());
		LOG.info("kaveh, datum.getMetaData().toString() is: "+datum.getMetaData().toString());
		LOG.info("kaveh, the inlinks.toString() is : "+inlinks.toString());


		LOG.info("kaveh, the parse.getText() is: "+parse.getText());
		LOG.info("kaveh, the parse.toString() is: "+parse.toString());
		LOG.info("kaveh, the parse.getData().toString() is: "+parse.getData().toString());
		LOG.info("kaveh, the parse.getData().getParseMeta().toString() is: "+parse.getData().getParseMeta().toString());
		

		return doc;
	}

*/
}
