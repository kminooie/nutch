package com.doslocos.nutch.harvester;


import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.w3c.dom.DocumentFragment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TrainingPart implements HtmlParseFilter{

	public static final Logger LOG = LoggerFactory.getLogger(TrainingPart.class);

	private Harvester harvester;	


	public TrainingPart() {
		LOG.info( "instantiating Harvester" );
		harvester = new Harvester();
	}
	
	
	@Override
	public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {

		String HTMLBody = new String(content.getContent());

		try {
			URL netUrl = new URL(content.getUrl());

			String pathName = netUrl.getPath();
			String hostName = netUrl.getHost();

			harvester.learn( HTMLBody, hostName, pathName );
			
		} catch (MalformedURLException e) {
			LOG.error("Error while training part in harvester plugin", e );
		}


		parseResult.get(content.getUrl()).getData().getParseMeta().add("rawcontent", HTMLBody );


		LOG.debug("harvester training part finished for : "+content.getUrl());
		return parseResult;
	}


	public void setConf(Configuration conf) {
		LOG.info( "setConf of TrainingPart was called." );
		
		if( Settings.setConf( conf ) ) {
			LOG.info( "Initializing Harvester" );
			harvester.init();
		}		
	}

	public Configuration getConf() {
		return Settings.getConf();
	}


	protected void finalize() {
		System.err.println( "trainingPart finalize was called" );
		LOG.info( "trainingPart finalize was called." );
	}
}
