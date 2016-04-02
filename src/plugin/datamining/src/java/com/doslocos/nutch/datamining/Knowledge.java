package com.doslocos.nutch.datamining;


import java.sql.SQLException;


public interface Knowledge {


	public boolean incNodeFreq( String host,String xpath, int hash ) throws SQLException;

	public int getNodeFreq( String domain,String xpath, String content );

	public long addUrlHostDb(String hostName, String pathName);

	boolean addIncNode(String domain, String path, String xpath, String content, long tempUrlId);

	boolean addNode(String domain, String path, String xpath, String content, long tempUrlId);


	
	public int getHostId( String host );
	
	public int getPathId( int hostId, String path );
	
	public boolean addNode( int hostId, int pathId, int hash, String xpath );
	
	public int getNodeFreq( int hostId, int hash, String xpath );
}

