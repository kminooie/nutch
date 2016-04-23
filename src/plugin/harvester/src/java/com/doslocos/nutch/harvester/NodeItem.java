/**
 * Class to be used to cache 
 * 
 * */

package com.doslocos.nutch.harvester;



public class NodeItem {
	public Integer xpathId;
	public Integer hash;
	
	public NodeItem() {
		
	}
	
	public NodeItem( Integer nodeXpathId, Integer nodeHashCode ) {
		xpathId = nodeXpathId;
		hash = nodeHashCode;
	}
} 
