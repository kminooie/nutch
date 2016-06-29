package com.doslocos.nutch.harvester;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;


public class Settings {
	static public final String CONF_PREFIX = "doslocos.harvester.";
	static public final Logger LOG = LoggerFactory.getLogger( Settings.class );

	static private Configuration conf;
	static private boolean inited = false;

	
	static public class Frequency {
		static public final String CONF_PREFIX = Settings.CONF_PREFIX + "frequency.";
		static public final Logger LOG = LoggerFactory.getLogger( Frequency.class );
		
		/**
		 * @var int * various frequency thresholds
		 */
		static public int collect, write, max, gc;
		
		static public void init() {
			collect =  conf.getInt( CONF_PREFIX + "collect" , 2  );
			LOG.info( "collect frequency threshold: " + collect );

			write =  conf.getInt( CONF_PREFIX + "write" , collect  );
			LOG.info( "write frequency threshold: " + write );

			gc =  conf.getInt( CONF_PREFIX + "gc" , 10 );
			LOG.info( "gc frequency threshold: " + gc );

			max =  conf.getInt( CONF_PREFIX + "max" , gc + 1  );
			LOG.info( "max frequency threshold: " + max );
		}
	}
	
	
	static public class Cache {
		static public final String CONF_PREFIX = Settings.CONF_PREFIX + "cache.";
		static public final Logger LOG = LoggerFactory.getLogger( Cache.class );
		
		/**
		 * @var int cache_* various cache tuning
		 */
		static public int hosts_per_job ,nodes_per_page;
		static public float load_factor;
		
		static public void init() {
			hosts_per_job = conf.getInt( CONF_PREFIX + "hosts_per_job", 2048 );
			LOG.info( "cache hosts_per_job: " + hosts_per_job );
			
			nodes_per_page = conf.getInt( CONF_PREFIX + "nodes_per_page", 4096 );
			LOG.info( "cache nodes_per_page: " + nodes_per_page );

			load_factor = conf.getFloat( CONF_PREFIX + "load_factor", 0.75f );
			LOG.info( "cache_load_factor: " + load_factor );
		}
		
		static public int getInitialCapacity( int items ) {
			return (int) Math.ceil( items / load_factor ) + 16;
		}
	}


	static public class NodeUtil {
		static public final String CONF_PREFIX = Settings.CONF_PREFIX + "util.";
		static public final Logger LOG = LoggerFactory.getLogger( NodeUtil.class );
		
		static public String removeList;
		
		static public void init() {
			removeList = conf.get( CONF_PREFIX + "remove_list", 
				"server,appserver,meta,link,timestamp,noscript,script,style,form,option,input,select,button,comment,#comment,#text,.hidden" 
			);
			
			LOG.info( "remove_list: " + removeList );
		}
	}


	static public class Storage {

		static public final String SEPARATOR = ":";
		static public String connClassName;
		static public String testHost = "redis.io";


		static public class Redis {
			static public final String CONF_PREFIX = Settings.CONF_PREFIX + "redis.";
			static public final Logger LOG = LoggerFactory.getLogger( Redis.class );
			
			static public final JedisPoolConfig poolConfig = new JedisPoolConfig();
			static public final ScanParams scanParams = new ScanParams();
			
			static public int port, db, timeOut, bucketSize;
			static public String host;
			
			static public void init() {
				host = conf.get( CONF_PREFIX + "host", "localhost" );
				port = conf.getInt( CONF_PREFIX + "port", 6379 );
				db = conf.getInt( CONF_PREFIX + "db", 0 );
				timeOut = conf.getInt( CONF_PREFIX + "timeOut", 0 );
				
				LOG.info( "Redis host:" + host + ":" + port +" db:"+ db + " timeout:" + timeOut );
				
				poolConfig.setTestOnBorrow( conf.getBoolean( CONF_PREFIX + "setTestOnBorrow", true ) );
				poolConfig.setTestOnReturn( conf.getBoolean( CONF_PREFIX + "setTestOnReturn", true ) );
				poolConfig.setTestWhileIdle( conf.getBoolean( CONF_PREFIX + "setTestWhileIdle", false ) );
				
				poolConfig.setMaxTotal( conf.getInt( CONF_PREFIX + "setMaxTotal", 16 ) );
				poolConfig.setMaxIdle( conf.getInt( CONF_PREFIX + "setMaxIdle", 6 ) );
				
				LOG.info( "Pool config, getTestOnBorrow: " + poolConfig.getTestOnBorrow() );
				LOG.info( "Pool config, getTestOnReturn: " + poolConfig.getTestOnReturn() );
				LOG.info( "Pool config, getTestWhileIdle: " + poolConfig.getTestWhileIdle() );
				LOG.info( "Pool config, getMaxTotal: " + poolConfig.getMaxTotal() );
				LOG.info( "Pool config, getMaxIdle: " + poolConfig.getMaxIdle() );
								
				bucketSize = conf.getInt( CONF_PREFIX + "bucket_size", 1024 );
				scanParams.count( bucketSize );
				
				LOG.info( "Scan params, count: " + bucketSize );
			}
		}


		static public class Mariadb {
			static public final String CONF_PREFIX = Settings.CONF_PREFIX + "mariadb.";
			static public final Logger LOG = LoggerFactory.getLogger( Mariadb.class );
			
			static public int poolMaxIdle, poolMaxTotal, readBSize, writeBSize;
			static public String DBHOST, SCHEMA, USER, PASS;


			static public void init() {
				DBHOST = conf.get("doslocos.harvester.mariadb.host", "localhost" );
				SCHEMA = conf.get("doslocos.harvester.mariadb.schema", "nutch_harvester_db" );
				USER = conf.get("doslocos.harvester.mariadb.username", "root" );
				PASS = conf.get("doslocos.harvester.mariadb.password", "" );

				poolMaxTotal = conf.getInt("doslocos.harvester.mariadb.poolMaxTotal", 8 );
				poolMaxIdle = conf.getInt("doslocos.harvester.mariadb.poolMaxIdle", 2 );

				readBSize = conf.getInt("doslocos.harvester.mariadb.readBatchSize", 500 );
				writeBSize = conf.getInt("doslocos.harvester.mariadb.writeBatchSize", 2000 );


				LOG.info( "storage url: jdbc:mysql://"+DBHOST+"/"+SCHEMA+" with user: " + USER );
				LOG.info( "poolMaxTotal: " + poolMaxTotal + " poolMaxIdle: " + poolMaxIdle );
				LOG.info( "readBatchSize: " + readBSize + " writeBatchSize: " + writeBSize );

			}
		}


		static public void init() {
			connClassName = conf.get( CONF_PREFIX + "storage.class", null );
					
			if( connClassName.equals( "com.doslocos.nutch.harvester.storage.Redis" ) ) {
				Redis.init();
				
			} else if( connClassName.equals( "com.doslocos.nutch.harvester.storage.Mariadb" ) ) {
				Mariadb.init();
				
			} else {
				LOG.error( "storage class ( " + CONF_PREFIX + "storage.class ) is not set. this property is mandatory." );
				LOG.error( "available options are:" );
				LOG.error( "com.doslocos.nutch.harvester.storage.Mariadb" );
				LOG.error( "com.doslocos.nutch.harvester.storage.Redis" );
			}

			
		}
	}


	static public class IndexingPart {
		static public final String CONF_PREFIX = Settings.CONF_PREFIX + "indexing.";
		static public final Logger LOG = LoggerFactory.getLogger( IndexingPart.class );
		
		static public String fieldName;
		static public ArrayList< String > fieldsToRemove;
		
		static public void init() {
			fieldName = conf.get( CONF_PREFIX + "field_name" , "harvested" );
			fieldsToRemove = new ArrayList< String >( Arrays.asList( conf.getStrings( CONF_PREFIX + "remove_fileds", new String[0] ) ) );
						
			LOG.info( CONF_PREFIX + "field_name:" + fieldName );
			LOG.info( CONF_PREFIX + "remove_fileds:" + fieldsToRemove );
			fieldsToRemove.add( fieldName );
		}
	}


	static public synchronized boolean setConf( Configuration conf ) {
		if( inited ) {
			LOG.error( "should not be here" );
			return ! inited;
		}

		Settings.conf = conf;
		
		Frequency.init();
		Cache.init();
		Storage.init();		


		NodeUtil.init();
		IndexingPart.init();
		
		return inited = true;
	}
	
	static public Configuration getConf() {
		return conf;
	}
}
