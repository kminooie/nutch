package com.doslocos.nutch.datamining;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeTree {

	
	
	
	public static final Logger LOG = LoggerFactory.getLogger(NodeTree.class);	

	private List<NodeTree> children = null;
	private double value;
	private String name;


	//constructor a NodeTree object with value
	public NodeTree( String nodeName,double value)
	{
		this.children = new ArrayList<>();
		this.value = value;
		this.name=nodeName;
		LOG.info("valuetree created. "+ nodeName);
	}


	//create a node without value
	public NodeTree( String nodeName)
	{
		this.children = new ArrayList<>();
		this.value = 0;
		this.name=nodeName;
		LOG.info("kaveh, valuetree created. "+ nodeName);
	}

	//add children with value
	public NodeTree addChild(String nodeName,double value )
	{
		NodeTree NodeTree=new NodeTree(nodeName,value);
		children.add((NodeTree) NodeTree);
		return NodeTree;
	}


	//add children to a node with 0 value
	public NodeTree addChild(String nodeName )
	{
		NodeTree NodeTree=new NodeTree(nodeName);
		children.add((NodeTree) NodeTree);
		return NodeTree;
	}


	//return value of node
	public double valueNode(){
		return this.value;
	}


	//return name of node
	public String nameNode(){
		return this.name;
	}


	//return number of children of node
	public int childrenSize(){
		return children.size();
	}

	//return all the children
	public List<NodeTree> children(){
		return this.children;
	}

	//add value to a node
	public void addValue(double value1){
		this.value=value1;
	}


	//return ith children of a node
	public NodeTree child(int i){

		try{

			return this.children.get(i);

		}catch (java.lang.IndexOutOfBoundsException | java.lang.NullPointerException e){
			LOG.info("the number of children is out of reach!: " + e);
			return null;
		}
	}


	//this function print the name and value of a node and its children
	public void printIt(NodeTree nodeprint ){
		//String print=nodeprint.valueNode()+"   "+nodeprint.nameNode();
		if (nodeprint.childrenSize()>0){
			for (int k=0;k<nodeprint.childrenSize();k++){
				printIt(nodeprint.child(k));

			}
		}

		LOG.info(nodeprint.valueNode()+"   "+nodeprint.nameNode());
	}

	
}
