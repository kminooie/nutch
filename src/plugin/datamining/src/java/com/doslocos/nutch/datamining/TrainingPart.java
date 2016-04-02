package com.doslocos.nutch.datamining;


import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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

	public static ParsingText kbc;
	//public static ParsingText;
	public static Node node;
	public static URL netUrl;
	public static String HTMLBody;
	public static Map<Long , String> url_ID;
	public long tempUrlId ;

	//	public TrainingPart() {
	//
	//	}



	@Override
	public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {

		ConnectMysql2.resetCounter();

		//to extract the content of a page
		String HTMLBody = new String(content.getContent());

		//node=parseObj.parseDom(HTMLBody);
		node = kbc.parseDom( HTMLBody );

		try {
			netUrl = new URL(content.getUrl());
		} catch (MalformedURLException e) {
			LOG.error("Error during extract url from content",e);

		}

		//this part added recently

		tempUrlId =kbc.addUrlHost(netUrl.getHost(), netUrl.getPath());
		
		url_ID = new HashMap<Long, String>();
		if (url_ID.containsValue(netUrl.getHost()+netUrl.getPath())){
		url_ID.put(tempUrlId, netUrl.getHost()+netUrl.getPath());
		}
		
		
		kbc.makeDatabase( node, "html/body", netUrl.getHost(), netUrl.getPath(), tempUrlId);
		Parse parse = parseResult.get(content.getUrl());
		Metadata metadata = parse.getData().getParseMeta();
		metadata.add( "rawcontent", HTMLBody );

		LOG.debug("datamining tarining part finished for : "+content.getUrl());
		LOG.info( "number of db roundtrip:" + ConnectMysql2.getCounter() );
		return parseResult;

	}


	public void setConf(Configuration conf) {

		Host_2locos_tariningpart=conf.get("doslocos.training.database.host");
		Schema_2locos_tariningpart=conf.get("doslocos.training.database.schema");
		USER_2locos_tariningpart=conf.get("doslocos.training.database.username");
		PASS_2locos_tariningpart=conf.get("doslocos.training.database.password");

		kbc = new ParsingText( Schema_2locos_tariningpart,Host_2locos_tariningpart,PASS_2locos_tariningpart,USER_2locos_tariningpart);
		TrainingPart.conf = conf;
	}


	public Configuration getConf() {

		return TrainingPart.conf;
	}





}
