package com.doslocos.nutch.datamining;


import java.sql.SQLException;


public interface Knowledge {

	public boolean addIncNode( String host,String path, String xpath, String content ) throws SQLException;

	public boolean addNode( String host,String path, String xpath, String content ) throws SQLException;

	public boolean incNodeFreq( String host,String xpath, int hash ) throws SQLException;

	public int getNodeFreq( String domain,String xpath, String content );


}

