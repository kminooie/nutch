
package com.doslocos.nutch.harvester;

public class NodeValue {
	
	static public final int SIZE = 2 * Integer.SIZE;
	
	public int frequency;
	public int dbId;
	
	public NodeValue( int freq ) {
		this( freq, 0 );
	}
	
	public NodeValue( int freq, int dbId ) {
		frequency = freq;
		this.dbId = dbId;
	}
	
	@Override
	public int hashCode() {
		return frequency ^ dbId;
	}
	
	
	@Override
	public boolean equals( Object obj ) {
		NodeValue rhs = ( NodeValue ) obj;
		return frequency == rhs.frequency && dbId == rhs.dbId; 
	}
	
	
	@Override
	public String toString() {
		return "frequency:" + frequency + " dbId:" + dbId;
	}

}
