package com.doslocos.nutch.datamining;


import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.jsoup.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;



public class TrainingPart implements HtmlParseFilter{

	public static final Logger LOG = LoggerFactory.getLogger(TrainingPart.class);


	// Database credentials
	public static String Schema_2locos_tariningpart;
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static String Host_2locos_tariningpart;
	public static Configuration conf;

	public static KnowledgeBaseCompare kbc;
	//public static ParsingText;
	public static Node node;
	public static URL netUrl;
	public static String HTMLBody;

	public TrainingPart() {

	}



	@Override
	public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {


		LOG.info("kaveh, the training part class of plug in called !");

		//to extract the content of a page
		String HTMLBody = new String(content.getContent());

		//node=parseObj.parseDom(HTMLBody);
		node = kbc.parseDom( HTMLBody );

		try {
			netUrl = new URL(content.getUrl());
		} catch (MalformedURLException e) {
			LOG.info("alireza, the error during extract url from content",e);

		}

		kbc.makeDatabase( node, "html/body", netUrl.getHost(), netUrl.getPath() );
		LOG.info("kaveh, the database for "+netUrl.getPath()+" done correctly");
		Parse parse = parseResult.get(content.getUrl());
		Metadata metadata = parse.getData().getParseMeta();
		LOG.info("kaveh, the content add in parse object in nutch : "+netUrl.getPath());
		metadata.add( "rawcontent", HTMLBody );

		return parseResult;

	}


	public void setConf(Configuration conf) {

		Host_2locos_tariningpart=conf.get("doslocos.training.database.host");
		Schema_2locos_tariningpart=conf.get("doslocos.training.database.schema");
		USER_2locos_tariningpart=conf.get("doslocos.training.database.username");
		PASS_2locos_tariningpart=conf.get("doslocos.training.database.password");

		kbc = new KnowledgeBaseCompare();
		TrainingPart.conf = conf;
	}


	public Configuration getConf() {

		return TrainingPart.conf;
	}





}
