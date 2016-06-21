package com.doslocos.nutch.harvester;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.doslocos.nutch.harvester.storage.Storage;
import com.doslocos.nutch.util.NodeUtil;

public class Settings {
	static public final String CONF_PREFIX = "doslocos.harvester.";
	static public final Logger LOG = LoggerFactory.getLogger( Settings.class );

	/**
	 * @var int ft_* various frequency thresholds
	 */
	static public int ft_collect, ft_write, ft_max, ft_gc ;
	
	/**
	 * @var int cache_* various cache tuning
	 */
	static public int cache_hosts_per_job ,cache_nodes_per_page;
	static public float cache_load_factor;
	
	static private String connClassName = null;
	static private Class<?> connClass;

	static synchronized public void setConf( Configuration conf ) {
		if( null != Storage.mainCache ) {
			LOG.error( "should not be here" );
			return;
		}
	
		String prefix = CONF_PREFIX;

		// parser settings
		NodeUtil.removeList = conf.get( prefix + "remove_list", NodeUtil.removeList );
		LOG.info( "selectList: " + NodeUtil.removeList );


		// frequency settings
		prefix += "frequency.";
		
		ft_collect =  conf.getInt( prefix + "collect" , 2  );
		LOG.info( "collect frequency threshold: " + ft_collect );

		ft_write =  conf.getInt( prefix + "write" , ft_collect  );
		LOG.info( "write frequency threshold: " + ft_write );

		ft_max =  conf.getInt( prefix + "max" , ft_write  );
		LOG.info( "max frequency threshold: " + ft_max );

		ft_gc =  conf.getInt( prefix + "gc" , 10 );
		LOG.info( "gc frequency threshold: " + ft_gc );


		// cache settings
		prefix = CONF_PREFIX + "cache.";
		cache_nodes_per_page = conf.getInt( prefix + "hosts_per_job", 2048 );
		LOG.info( "cache_nodes_per_page: " + cache_hosts_per_job );
		
		cache_nodes_per_page = conf.getInt( prefix + "nodes_per_page", 4096 );
		LOG.info( "cache_nodes_per_page: " + cache_nodes_per_page );

		cache_load_factor = conf.getFloat( prefix + "load_factor", 0.95f );
		LOG.info( "cache_load_factor: " + cache_load_factor );

		Storage.mainCache = new ConcurrentHashMap< Integer, HostCache >( cache_hosts_per_job );

		// storage settings
		if( null != ( connClassName = conf.get( CONF_PREFIX + "storage.class", null ) ) ) {
			try {
				LOG.info( "storage class: " + connClassName );
				connClass = Class.forName( connClassName );

				Method setConf = connClass.getMethod( "setConf", Configuration.class );
				setConf.invoke( null, conf );

			} catch( Exception e ) {
				LOG.error( "Got exception while setting storage class: ", e );
				die();
			}
		} else {
			LOG.error( "storage class ( " + CONF_PREFIX + "storage.class ) is not set. this property is mandatory." );
			LOG.error( "available options are:" );
			LOG.error( "com.doslocos.nutch.harvester.storage.Mariadb" );
			LOG.error( "com.doslocos.nutch.harvester.storage.Redis" );

			die();
		}
	}
	
}
