package com.doslocos.nutch.datamining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.jsoup.nodes.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingPart implements IndexingFilter {

	public static final Logger LOG = LoggerFactory.getLogger(IndexingPart.class);
	private Configuration conf;
	public static int schema_2locos_frequency_threshould;
	public static String Schema_2locos_tariningpart;
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static String Host_2locos_tariningpart;
	ParsingText nodeParse;

	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException  {




		//assign the value of frequency from nutch-site to the variable in parsingpart class 
		ParsingText.frequency_threshould=schema_2locos_frequency_threshould;

		//parsing web page
		Node nodePage=nodeParse.parseDom(parse.getData().getParseMeta().get("rawcontent"));
		//compare web page with database
		String textContent=ParsingText.compareKB(nodePage, "html/body",doc.getFieldValue("host").toString());

		//removing old parsed text and adding the new one
		doc.removeField("content");
		doc.add("content", textContent);
		LOG.debug("new parsed text replaced with old one by datamining plug in for : "+url.toString());
		//remove temporary field from metadata
		parse.getData().getParseMeta().remove("rawcontent");


		return doc;
	}



	public void setConf(Configuration conf) {

		schema_2locos_frequency_threshould=Integer.parseInt(conf.get("doslocos.frequency.threshould"));
		Host_2locos_tariningpart=conf.get("doslocos.training.database.host");
		Schema_2locos_tariningpart=conf.get("doslocos.training.database.schema");
		USER_2locos_tariningpart=conf.get("doslocos.training.database.username");
		PASS_2locos_tariningpart=conf.get("doslocos.training.database.password");

		//parsing the web page
		nodeParse=new ParsingText(Schema_2locos_tariningpart,Host_2locos_tariningpart,PASS_2locos_tariningpart,USER_2locos_tariningpart);
		this.conf = conf;

	}

	public Configuration getConf() {
		return this.conf;
	}




}
