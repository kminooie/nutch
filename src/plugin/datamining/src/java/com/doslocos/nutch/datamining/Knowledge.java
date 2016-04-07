package com.doslocos.nutch.datamining;

public abstract class Knowledge {
	
	static int counter = 0;

	public abstract  boolean addNode( int hashcode, int hostId, int pathId, int xpathId );

	public abstract int readFreqNode(int hashcodeNode, int hostId , int xpathId );

	public abstract int getHostId( String host);

	public abstract int getPathId(String path);

	public abstract int getXpathId(String xpath);




}

