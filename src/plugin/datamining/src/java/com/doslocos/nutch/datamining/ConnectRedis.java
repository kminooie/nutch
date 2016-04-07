package com.doslocos.nutch.datamining;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class ConnectRedis extends Knowledge {


	public static final Logger LOG = LoggerFactory.getLogger( ConnectRedis.class );

	public static String hostAdd;
	public static int dbNum;

	public static Jedis jedis;

	public ConnectRedis(Configuration conf){

		hostAdd = conf.get("doslocos.training.redisDb.urlConnection");
		dbNum = Integer.parseInt(conf.get("doslocos.training.redisDb.dbNum"));
		initConnection(hostAdd, dbNum);
	}

	
	//connect to database
	private static boolean initConnection( String hostAdd , int dbNum ){

		boolean result = false ;

		try{

			jedis=new Jedis( hostAdd );
			jedis.select( dbNum );

			result = true ;

		}catch( Exception e ){

			LOG.error( "there is an error during connect to database" + e);
			//the program must be killed here
		}
		LOG.debug(" Connect Jedis is established!");


		return result;

	}

	
	@Override	
	public int getHostId( String host){


		int result = 0 ;
		String host_ID=jedis.get( host );

		if (host_ID == null ){

			result = host.hashCode();
			counter++;

			jedis.set( host, Integer.toString( result ) );

		}else{

			result = Integer.parseInt( ( host_ID ) );
		}



		return result;


	}


	@Override
	public int getPathId(int hostId, String path) {
		int result = 0 ;
		String path_ID = jedis.get( path );

		if ( path_ID == null ){

			result = path.hashCode();
			counter++;

			jedis.set( path , Integer.toString( result ) );

		}else{

			result = Integer.parseInt( ( path_ID ) );
		}
		
		return result;
	}

	@Override
	public boolean addNode(int hostId, int pathId, int hash, String xpath) {
		boolean result = false;

		String xpathHashCode = Integer.toString( xpath.hashCode() );
		
		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( hostId ) + "_" + xpathHashCode;
		
		String nodeId = Integer.toString( nodeKey.hashCode() );
		
		String oldNodeId = jedis.getSet( nodeKey, nodeId ); 
		
		if( null == oldNodeId ) {
			result = true;
		} if( oldNodeId != nodeId ) {
			LOG.error( "Same node detected with diferent hash code old:" + oldNodeId + " new:" + nodeId );
		}	
		
		jedis.sadd( nodeId, Integer.toString( pathId ));

		counter += 2;

		return result;
	}

	@Override
	public int getNodeFreq(int hostId, int hash, String xpath) {
		int freq = 0;

		String xpathHashCode = Integer.toString( xpath.hashCode() );
		
		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( hostId ) + "_" + xpathHashCode;
		
		String nodeId = Integer.toString( nodeKey.hashCode() );

		
		freq = jedis.scard( nodeId ).intValue();
		counter++;

		return freq;
	}


}
