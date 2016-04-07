package com.doslocos.nutch.datamining;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class ConnectJedis extends Knowledge{


	public static final Logger LOG = LoggerFactory.getLogger( ConnectJedis.class );

	public static String hostAdd;
	public static int dbNum;

	public static Jedis jedis;

	public ConnectJedis(Configuration conf){

		hostAdd = conf.get("doslocos.training.redisDb.urlConnection");
		dbNum = Integer.parseInt(conf.get("doslocos.training.redisDb.dbNum"));
		initConnection(hostAdd, dbNum);
	}

	//Add a node in database

	public  boolean addNode( int hashcode, int hostId, int pathId, int xpathId ){


		boolean result = false;


		String NodeID = getNodeId( hashcode , hostId , xpathId );

		try{


			jedis.sadd( NodeID, Integer.toString( pathId ));

			if (jedis.scard(NodeID)>1){
				result = true;
			}


		}catch( Exception e ){
			LOG.error( "error while adding a node in database" );
		}

		LOG.info("the node with  "+NodeID+ " id add to database and it existed in database :"+result);

		return result;
	}



	//read a frequency of a node
	public int readFreqNode(int hashcodeNode, int hostId , int xpathId ){

		int freq = 0;

		String NodeID = getNodeId( hashcodeNode , hostId , xpathId );

		if( NodeID != null ){

			freq = jedis.scard(NodeID).intValue();

		}
		
		LOG.info("the frequency of : "+NodeID+" is :"+freq);

		return freq;
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
		LOG.info(" Connect Jedis is stablished!");


		return result;

	}


	public int getHostId( String host){


		int result = 0 ;
		String host_ID=jedis.get( host );

		if (host_ID == null ){

			result = host.hashCode();

			jedis.set( host, Integer.toString( result ) );

		}else{

			result = Integer.parseInt( ( host_ID ) );
		}

		LOG.info("the HostID of : "+host+" is : "+result);


		return result;


	}



	public int getPathId(String path){


		int result = 0 ;
		String path_ID = jedis.get( path );

		if ( path_ID == null ){

			result = path.hashCode();

			jedis.set( path , Integer.toString( result ) );

		}else{

			result = Integer.parseInt( ( path_ID ) );
		}

		LOG.info("the PathID of : "+path+" is : "+result);

		
		return result;


	}



	public int getXpathId(String xpath){


		int result = 0 ;
		String xpath_ID = jedis.get( xpath );

		if ( xpath_ID == null ){

			result = xpath.hashCode();

			jedis.set( xpath , Integer.toString( result ) );

		}else{

			result = Integer.parseInt( ( xpath_ID ) );
		}

		LOG.info("the XpathID of : "+xpath+" is : "+result);

		
		return result;


	}


	//get NodeId by host, xpath,and node.hashcode()
	private static String getNodeId( int hash, int host_ID, int xpath_ID ){



		String nodeKey = Integer.toString( hash ) + "_" + Integer.toString( host_ID ) + "_" + Integer.toString( xpath_ID );
		String nodeId=jedis.get( nodeKey );

		if ( nodeId == null ){

			nodeId = Integer.toString((
					Integer.toString( hash ) + Integer.toString( host_ID ) + Integer.toString( xpath_ID ) ).hashCode()) ;

			jedis.set( nodeKey,Integer.toString( nodeKey.hashCode() ) );

			nodeId = Integer.toString( nodeKey.hashCode() );
		}

		LOG.info("the NodeID of : "+nodeKey+" is : "+nodeId);

		
		return nodeId;


	}


}
