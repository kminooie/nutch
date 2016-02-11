package org.apache.nutch.parse.html_a;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.nutch.parse.html_a.Knowledge;

public class ConnectMysql implements Knowledge {


	//the variable and constant 

	public String JDBC_DRIVER = "org.mariadb.jdbc.Driver";  
	public String DB_URL = "jdbc:mysql://localhost/KnowledgeBase";

	// Database credentials
	public String USER = "pasha";
	public String PASS = "2014";

	private Connection conn=null;
	private  Statement stmt = null;



	//constructor 
	public ConnectMysql(String USER,String PASS,String JDBC_DRIVER,String DB_URL) throws ClassNotFoundException, SQLException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = conn.createStatement();
	}


	@Override
	public boolean addIncNode(String host, String path, String xpath, String content) throws SQLException {
		// TODO Auto-generated method stub

		ResultSet rs = stmt.executeQuery("select * from pages where host='"+host+"'  and xpath='"+xpath+"' and  content='"+content+"';");
		if(rs.next()) { 
			incNodeFreq(host, xpath,content.hashCode());
			//return true if node exist and increase frequency
			return true;

		}else{
			addNode(host,path,xpath,content);
			//return false and put the node in database
			return false;
		}

	}

	//this function add a node in database  done!
	@Override
	public boolean addNode(String host, String path, String xpath, String content) throws SQLException {
		// TODO Auto-generated method stub

		stmt.executeUpdate("insert ignore into  pages (host ,xpath, hash,frequency,content,path)values ('"+host+"' , '"+xpath+"' , "+ content.hashCode()+" , 1 ,'"+ content+"' ,'"+ path+"');");

		return false;
	}


	//just increase the frequency field of a node  done!
	@Override
	public boolean incNodeFreq(String host, String xpath, int hash) throws SQLException {
		// TODO Auto-generated method stub
		stmt.executeUpdate("update pages set frequency=frequency+1  where host='"+host+"' and xpath='"+xpath+"' and hash= "+hash+" ;");

		return false;
	}


	@Override
	public Map<String, Object> getNode(String host, String xpath, int hash) {
		// TODO Auto-generated method stub
		return null;
	}


	//use this function to check the connection at the end and if the connections were open this function close them.

	protected void finalize() throws SQLException{

		if (stmt != null) {
			stmt.close();
		}
		if (conn != null) {
			conn.close();
		}

	}



}




