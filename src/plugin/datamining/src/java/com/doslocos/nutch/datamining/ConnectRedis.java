package com.doslocos.nutch.datamining;


import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


public class ConnectRedis extends Knowledge {


	public static final Logger LOG = LoggerFactory.getLogger( ConnectRedis.class );

	private static String hostAdd; 
	private static int redisPort, dbNumber, dbTimeOut, setMaxTotal, setMaxIdle;
	private static boolean setTestOnBorrow, setTestOnReturn, setTestWhileIdle;
	private int frequency_treshold;
	public static JedisPool pool;
	public static JedisPoolConfig poolConfig;

	public Jedis jedis;

	public ConnectRedis(Configuration conf){

		if( null == poolConfig) {

			hostAdd = conf.get( "doslocos.training.redisDb.urlConnection", "127.0.0.1" );
			redisPort = conf.getInt( "doslocos.training.redisDb.portNumber", 6379 );
			dbNumber = conf.getInt( "doslocos.training.redisDb.dbNumber", 0 );
			dbTimeOut = conf.getInt( "doslocos.training.redisDb.dbTimeOut", 0 );
			setTestOnBorrow = conf.getBoolean( "doslocos.training.redisDb.setTestOnBorrow", true );
			setTestOnReturn = conf.getBoolean( "doslocos.training.redisDb.setTestOnReturn", true );
			setTestWhileIdle = conf.getBoolean( "doslocos.training.redisDb.setTestWhileIdle", true );
			setMaxTotal = conf.getInt( "doslocos.training.redisDb.setMaxTotal",0);
			setMaxIdle = conf.getInt( "doslocos.training.redisDb.setMaxIdle",0);

			frequency_treshold = conf.getInt( "doslocos.training.frequency_threshould" , 2  );

			poolConfig = new JedisPoolConfig();

			poolConfig.setTestOnBorrow( setTestOnBorrow );
			poolConfig.setTestOnReturn( setTestOnReturn );
			poolConfig.setTestWhileIdle( setTestWhileIdle);
			poolConfig.setMaxTotal( setMaxTotal ); 
			poolConfig.setMaxIdle( setMaxIdle );  

			pool = new JedisPool( poolConfig, hostAdd, redisPort, dbTimeOut );	

		}

		initConnection();
	}


	private boolean initConnection() {
		boolean result = false ;

		try{			
			if( null == jedis || ! jedis.isConnected() ) {
				jedis = pool.getResource(); 
				jedis.select( dbNumber );
			}

			result = true ;

		}catch( Exception e ){

			LOG.error( "there is an error during connect to database", e );
		}

		LOG.debug( "initConnection is returnig:" + result );
		return result;

	}



	@Override	
	public int getHostId( String host){
		//		int result = 0 ;
		//		String host_ID=jedis.get( host );
		//
		//		if (host_ID == null ){
		//
		//			result = host.hashCode();
		//			counter++;
		//
		//			jedis.set( host, Integer.toString( result ) );
		//
		//		}else{
		//
		//			result = Integer.parseInt( ( host_ID ) );
		//		}
		//
		//
		//
		//		return result;



		int hostID = host.hashCode();

		return hostID;
	}


	@Override
	public int getPathId(int hostId, String path) {


		//		int result = 0 ;
		//		String path_ID = jedis.get( path );
		//
		//		if ( path_ID == null ){
		//
		//			result = path.hashCode();
		//			counter++;
		//
		//			jedis.set( path , Integer.toString( result ) );
		//
		//		}else{
		//
		//			result = Integer.parseInt( ( path_ID ) );
		//		}

		//		return result;

		int pathID = path.hashCode();
		return pathID;
	}

	@Override
	public boolean addNode(int hostId, int pathId, int hash, String xpath) {

		boolean result = false;
		boolean achived = false;

		String xpathHashCode = Integer.toString( xpath.hashCode() );

		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( hostId ) + "_" + xpathHashCode;

		String PathIdString = Integer.toString( pathId ) ;


		int times = counter;

		while( ! achived ) {
			try{

				counter ++;

				int  freqPath = jedis.sadd( nodeKey.getBytes(), PathIdString.getBytes()).intValue();
				if( 1 == freqPath ) {
					int freq = jedis.scard(nodeKey.getBytes()).intValue();
					counter ++;
					if(freq < (frequency_treshold+1)){
						result=true;
						
					}
				}


				achived = true;
			}catch(Exception e){

				LOG.error("error happen here for :"+ nodeKey +" pathId is: "+ PathIdString+ " xpath is: "+ xpath+" result is " + result+"  "+ e );


				initConnection();
				if( counter > times + 2 ) {
					LOG.error( "Can't resolve the issue by reinitilizing the connection" );
					achived = true;
				}
			}
		}
		return result;
	}

	@Override
	public int getNodeFreq(int hostId, int hash, String xpath) {


		int freq = 0;

		String xpathHashCode = Integer.toString( xpath.hashCode() );

		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( hostId ) + "_" + xpathHashCode;


		try{
			freq = jedis.scard( nodeKey.getBytes() ).intValue();

		}catch(Exception e){

			LOG.debug("error ocure during get node frequency"+"  ", e );

		}
		counter++;

		return freq;
	}


	protected void finalize(){
		if( null != jedis ) {
			try {
				jedis.close();
			} catch ( Exception e ) {
				LOG.error( "Exception whild closing the jedis connection.", e );
			}
			jedis = null;
		}

	}

}
