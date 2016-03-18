package org.apache.nutch.indexwriter.solr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectMysql  implements ConnectionKB {

	public static final Logger LOG = LoggerFactory.getLogger(ConnectMysql.class);	


	private static Connection conn = null;

	public static  PreparedStatement psSelect = null;

	//constructor 
	public ConnectMysql(){

		LOG.info("kaveh, the ConnectMysql class created.");

		//call init function to initial a connection to database
		initConnection();
	}



	//connect to database
	private static boolean initConnection() {

		boolean result = true;

		if( null == conn ) {
			try {
				Class.forName("org.mariadb.jdbc.Driver");
				conn = DriverManager.getConnection("jdbc:mysql://localhost/KnowledgeBase","pasha","2014");
				LOG.info("connection successfully stablished" );
			} catch( Exception e ) {
				LOG.info( "Failed to establish connection:"+ e );
				result = false;
			}
		}

		return result;
	}


	//this function takes primaary keys of a node and returns the frequency of that node
	public int getNodeFreq( String domain,String xpath, String content ) {
		int result = 0;

		LOG.info("kaveh, looking for value of frequency: "+xpath);
		try {	
			/*
			LOG.info("kaveh, read from database xpath : "+xpath);
			LOG.info("kaveh, read from database content : "+content);
			LOG.info("kaveh, read from database domain : "+domain);
			*/
			psSelect=conn.prepareStatement( "SELECT frequency FROM pages where host=? and xpath=? and hash=?;" );
			psSelect.setString(1,domain);
			psSelect.setString(2, xpath);
			psSelect.setInt(3,content.hashCode());

			ResultSet rs=psSelect.executeQuery();

			if(rs.next()) {
				result = rs.getInt("frequency");
			}else{
				LOG.info( "cant find " + xpath + " node in database." );
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			LOG.error("Except During compare a node with database :"+e);

		}

		LOG.info("the value of frequency for "+xpath+"is : " + result );

		return result;
	}


}
