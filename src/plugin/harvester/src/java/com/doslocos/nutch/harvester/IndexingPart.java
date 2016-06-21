package com.doslocos.nutch.harvester;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingPart implements IndexingFilter {

	public static final Logger LOG = LoggerFactory.getLogger(IndexingPart.class);

	private String newFieldName = "harvested";
	private Configuration conf;
	private Harvester harvester;
	private String[] fieldsToRemove; 
	
	
	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException  {

		LOG.debug( "filter called with urt:" + url );

		URL netUrl = null;
		try {
			netUrl = new URL( doc.getFieldValue("url").toString() );
			LOG.info("extract the path: "+netUrl.getPath());
		} catch (MalformedURLException e) {
			LOG.error("Exception while extracting path from Url");
		}

		String textContent = harvester.filter( 
			parse.getData().getParseMeta().get("rawcontent"), 
			doc.getFieldValue("host").toString(), 
			netUrl.getPath() 
		);

		for (String val : fieldsToRemove) {
			doc.removeField( val );
		}

		doc.removeField( newFieldName );
		doc.add( newFieldName, textContent );

		LOG.debug("new parsed text replaced with old one by harvester plug in for : "+url.toString());

		return doc;
	}


	public void setConf(Configuration conf) {
		harvester = new Harvester();

		newFieldName =  conf.get( "doslocos.harvester.fieldname" , newFieldName );
		LOG.info( "doslocos.harvester.fieldname: " + newFieldName );
		
		fieldsToRemove = conf.getStrings("doslocos.harvester.removefileds", new String[0] );

		this.conf = conf;
	}


	public Configuration getConf() {
		return this.conf;
	}


	protected void finalize() {
		System.err.println( "IndexingPart finalize was called" );
		LOG.info( "IndexingPart finalize was called." );
	}

}
