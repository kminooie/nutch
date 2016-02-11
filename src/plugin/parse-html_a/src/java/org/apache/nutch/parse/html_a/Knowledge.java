package org.apache.nutch.parse.html_a;


import java.sql.SQLException;
import java.util.Map;

import org.jsoup.nodes.Node;

public interface Knowledge {
	
	public boolean addIncNode( String host,String path, String xpath, String content ) throws SQLException;

	public boolean addNode( String host,String path, String xpath, String content ) throws SQLException;

	public boolean incNodeFreq( String host,String xpath, int hash ) throws SQLException;

	public Map<String,Object> getNode( String host,String xpath, int hash );


}

