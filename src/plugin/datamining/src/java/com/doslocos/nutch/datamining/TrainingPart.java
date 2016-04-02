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
import org.w3c.dom.DocumentFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TrainingPart implements HtmlParseFilter{

	public static final Logger LOG = LoggerFactory.getLogger(TrainingPart.class);
	public static Configuration conf;
	public static ParsingText kbc;
	

	@Override
	public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {

		//to extract the content of a page
		String HTMLBody = new String(content.getContent());

		try {
			URL netUrl = new URL(content.getUrl());
			kbc.learn( HTMLBody, netUrl.getHost(), netUrl.getPath() );
		} catch (MalformedURLException e) {
			LOG.error("Error while extracting url from content", e );
		}
				

		//pass the content to indexing
		Parse parse = parseResult.get(content.getUrl());
		Metadata metadata = parse.getData().getParseMeta();
		metadata.add( "rawcontent", HTMLBody );

		LOG.debug("datamining tarining part finished for : "+content.getUrl());
		return parseResult;
	}

	public void setConf(Configuration conf) {
		if( null == TrainingPart.conf ) {
			TrainingPart.conf = conf;
			kbc = new ParsingText( conf);
		}
	}

	public Configuration getConf() {
		return TrainingPart.conf;
	}

}
