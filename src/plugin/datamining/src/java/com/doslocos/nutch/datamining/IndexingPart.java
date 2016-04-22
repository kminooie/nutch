package com.doslocos.nutch.datamining;

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
	private Configuration conf;
	private ParsingText nodeParse;

	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException  {

		String textContent = nodeParse.filter( parse.getData().getParseMeta().get("rawcontent"), doc.getFieldValue("host").toString() );

		
		doc.add("rawcontent", textContent);

		LOG.debug("new parsed text replaced with old one by datamining plug in for : "+url.toString());

		return doc;
	}



	public void setConf(Configuration conf) {
		nodeParse = new ParsingText( conf );
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}




}
