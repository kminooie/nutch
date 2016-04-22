package com.doslocos.nutch.datamining;

public abstract class Knowledge {
	public int counter = 0;
	public static int bCounter=0;
	
	public static int strToId( String str ) {
		return str.hashCode();
	}
	
	public abstract int getHostId( String host );
	
	public abstract int getPathId( int hostId, String path );
	
	
	/**
	 * 
	 * @param hostId
	 * @param pathId
	 * @param hash
	 * @param xpath
	 * @return boolean return false if node already existed, true if it has been added
	 */
	public abstract boolean addNode( int hostId, int pathId, int hash, String xpath );
	
	public abstract int getNodeFreq( int hostId, int hash, String xpath );
	
	public abstract boolean emptyBatch(int pathId);

}

