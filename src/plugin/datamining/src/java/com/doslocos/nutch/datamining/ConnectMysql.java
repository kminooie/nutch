package com.doslocos.nutch.datamining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectMysql implements Knowledge {

	@Override
	public boolean incNodeFreq(String host, String xpath, int hash) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getNodeFreq(String domain, String xpath, String content) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long addUrlHostDb(String hostName, String pathName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean addIncNode(String domain, String path, String xpath, String content, long tempUrlId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNode(String domain, String path, String xpath, String content, long tempUrlId) {
		// TODO Auto-generated method stub
		return false;
	}


/*
	public static String Schema_2locos_tariningpart;
	public static String USER_2locos_tariningpart ;
	public static String PASS_2locos_tariningpart;
	public static String Host_2locos_tariningpart;

	private static Connection conn = null;
	public static  PreparedStatement psSelect = null;
	public static  PreparedStatement psInsert = null;
	public static  PreparedStatement psUpdate = null;

	public static final Logger LOG = LoggerFactory.getLogger( ConnectMysql.class );

	
	public int counter = 0;
	
	public void resetCounter() {
		counter = 0;
	}
	
	public void incCounter() {
		++counter;
	}
	
	public int getCounter() {
		return counter;
	}

	//constructor 
	public ConnectMysql(String schema2locos,String host2locos,String pass2locos,String user2locos){
		// TODO read db credentials from nutch conf object 

		Schema_2locos_tariningpart=schema2locos;
		USER_2locos_tariningpart =user2locos;
		PASS_2locos_tariningpart=pass2locos;
		Host_2locos_tariningpart=host2locos;

		LOG.debug("Connection class called");
		initConnection( false );
	}


	private static void checkConnection() {
		boolean renew = false;
		try {
			if ( conn.isClosed() ) renew = true;
		} catch ( Exception e ) {
			LOG.error( "Exception while trying to check connection:", e );
			renew = true;
		}

		if( renew ) {
			renew = initConnection( true );
			LOG.info("kaveh, checkConnection called initConnection");

		}

		if( ! renew ) {
			// TODO die here
		}
	}

	private static boolean initConnection( boolean force ) {
		if( force || null == conn ) {
			try {
				LOG.debug("Connection to database stablished");
				Class.forName("org.mariadb.jdbc.Driver");
				String sqlConnection="jdbc:mysql://"+Host_2locos_tariningpart+"/"+Schema_2locos_tariningpart;
				conn = DriverManager.getConnection(sqlConnection, USER_2locos_tariningpart, PASS_2locos_tariningpart);
			} catch( Exception e ) {
				LOG.error( "Failed to establish connection:", e );
				return false;
			}
		}

		return true;
	}



	@Override
	public boolean addIncNode(String domain, String path, String xpath, String content) {

		boolean result = addNode( domain, path, xpath, content ); 

		if( ! result ) {
			incNodeFreq( domain, xpath, content.hashCode() );

		}

		return result;
		//result = false means node increased
		//result = true means node added




		/*
		boolean result = addNode( domain, path, xpath, content ); 

		if( ! result ) {
			result = incNodeFreq( domain, xpath, content.hashCode() );
		}

		return result;
	
	}



	//this function add a node in database  done!
	@Override
	public boolean addNode(String domain, String path, String xpath, String content) {

		boolean result = false;

		checkConnection();
		try {

			if( null == psSelect ) {
			//	psSelect = conn.prepareStatement( "INSERT INTO pages (host,xpath,hash,frequency,content,path) VALUES (?, ?, ?, 1, ?, ?);" );

				psSelect = conn.prepareStatement( "INSERT INTO pages (host,xpath,hash,frequency) VALUES (?, ?, ?, 1);" );
			}

			psSelect.setString(1,domain);
			psSelect.setString(2,xpath);
			psSelect.setInt(3, content.hashCode());
			//psSelect.setString(4,content);
			//psSelect.setString(4,path);

			psSelect.executeUpdate();
			result = true;
			
			incCounter();
		} catch( java.sql.SQLIntegrityConstraintViolationException e ) {

			//LOG.info( "Got SQLIntegrity exception assume node already exist hash:" + xpath );
			result = false; // Redundant

		}catch( java.sql.BatchUpdateException e ) {

			//LOG.info( "alireza, Got BatchUpdateException exception assume node already exist hash:" + xpath );

			result = false; // Redundant

		} catch (SQLException e) {
			// TODO check for existing node is part of normal operation and not an error
			LOG.error( "Exception while adding a new node:", e );
		}

		return result;
	}



	//just increase the frequency field of a node  done!
	@Override
	public boolean incNodeFreq(String domain, String xpath, int hash) {

		boolean result = false;

		checkConnection();
		try {

			if( null == psUpdate ) {
				psUpdate = conn.prepareStatement( "update pages set frequency=frequency+1  where host= ? and xpath= ? and hash= ? ;" );
			}

			psUpdate.setString(1,domain);
			psUpdate.setString(2,xpath);
			psUpdate.setInt(3,hash);

			psUpdate.executeUpdate();

			result = true;
			
			incCounter();

		} catch (SQLException e) {
			LOG.error( "Exception while increasing the frequency:", e );
		}


		return result;
	}



	@Override
	public int getNodeFreq( String domain,String xpath, String content ) {
		int result = 0;


		checkConnection();
		try {	

			psSelect=conn.prepareStatement( "SELECT frequency FROM pages where host=? and xpath=? and hash=?;" );
			psSelect.setString(1,domain);
			psSelect.setString(2, xpath);
			psSelect.setInt(3,content.hashCode());

			ResultSet rs=psSelect.executeQuery();

			if(rs.next()) {
				result = rs.getInt("frequency");
			}else{
				LOG.debug( "cant find " + xpath + " node in database. and content is: "+ content );
			}
			
			incCounter();

		} catch (SQLException e) {

			LOG.error("Except During compare a node with database :"+xpath+"  "+e);

		}


		return result;
	}



	//use this function to check the connection at the end and if the connections were open this function close them.

	protected void finalize(){
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn = null;
		}
	}

	@Override
	public long addUrlHostDb(String hostName, String pathName) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean addIncNode(String domain, String path, String xpath, String content, long tempUrlId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNode(String domain, String path, String xpath, String content, long tempUrlId) {
		// TODO Auto-generated method stub
		return false;
	}

*/
}

