package com.doslocos.indexer.filtering;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.Parse;
import org.apache.hadoop.conf.*;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilteringXpath implements IndexingFilter{

	
	public static final Logger LOG = LoggerFactory.getLogger(FilteringXpath.class);
	
	private Configuration configuration;
	
	
	public Configuration getConf() {
		return configuration;
	}

	
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		
	}
	
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,CrawlDatum datum, Inlinks inlinks) throws IndexingException{
		 
		 
		 
		 LOG.info("hi alireza I am here!");
		 return doc;
	 }
	
	
	
	
}
