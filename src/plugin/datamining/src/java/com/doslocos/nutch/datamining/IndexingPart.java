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


	@Override
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException  {


		//parsing the web page
		ParsingPart nodeParse=new ParsingPart();

		//assign the value of frequency from nutch-site to the variable in parsingpart class 
		ParsingPart.frequency_threshould=schema_2locos_frequency_threshould;

		//parsing web page
		Node nodePage=nodeParse.parseDom(parse.getData().getParseMeta().get("rawcontent"));

		//compare web page with database
		String textContent=ParsingPart.compareKB(nodePage, "html/body",doc.getFieldValue("host").toString());

		//removing old parsed text and adding the new one
		doc.removeField("content");
		doc.add("content", textContent);

		//remove temporary field from metadata
		parse.getData().getParseMeta().remove("rawcontent");


		return doc;
	}



	public void setConf(Configuration conf) {

		schema_2locos_frequency_threshould=Integer.parseInt(conf.get("frequency_threshould"));
		Host_2locos_tariningpart=conf.get("doslocos.training.database.host");
		Schema_2locos_tariningpart=conf.get("doslocos.training.database.schema");
		USER_2locos_tariningpart=conf.get("doslocos.training.database.username");
		PASS_2locos_tariningpart=conf.get("doslocos.training.database.password");
		this.conf = conf;

	}

	public Configuration getConf() {
		return this.conf;
	}




}
