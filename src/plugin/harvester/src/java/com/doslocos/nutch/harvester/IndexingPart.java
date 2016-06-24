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

	
	private Harvester harvester;
	
	public IndexingPart() {
		LOG.info( "instantiating Harvester" );
		harvester = new Harvester();
	}


	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException  {

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

		for (String val : Settings.IndexingPart.fieldsToRemove ) {
			doc.removeField( val );
		}

		doc.add( Settings.IndexingPart.fieldName, textContent );

		LOG.debug( "filter finished for urt:" + url );

		return doc;
	}


	public void setConf(Configuration conf) {
		LOG.info( "setting configuration in IndexingFilter" );
				
		if( Settings.setConf( conf ) ) {
			LOG.info( "Initializing Harvester" );
			harvester.init();
		}		
	}


	public Configuration getConf() {
		return Settings.getConf();
	}


	protected void finalize() {
		System.err.println( "IndexingPart finalize was called" );
		LOG.info( "IndexingPart finalize was called." );
	}

}
