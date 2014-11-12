package P7;

import java.sql.*;

public class P7A1a {
	
	public static void main(String[] args)
	{
	//Create connection string
	String connectionURL = "jdbc:sqlserver://localhost:1433;"
	+ "databaseName=BenchmarkDB;user=test;password=123";
	
	//JDBC Objects
	Connection con = null;
	Statement stmt = null;
	ResultSet rs = null;
	
	//Establish the connection
	
	try {
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		con = DriverManager.getConnection(connectionURL);
		
		String SQLL = "DELETE FROM tellers;DELETE FROM accounts;DELETE FROM branches;";
		Statement st = con.createStatement();
		int l = st.executeUpdate(SQLL);
		st.close();
		//TODO N 
		int n = 10;
		
		for(int i = 1; i < n; i++)
		{
			String SQL = "INSERT INTO branches (branchid, branchname, balance, address) "+
					"VALUES ("+i+",'bcerabcerabcerabcer',0,'bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabr');";
			stmt = con.createStatement();
	        int v = stmt.executeUpdate(SQL);
	        //rs.close();
	        stmt.close();
		}
		for(int i = 1; i < n*100000; i++)
		{
			String SQL = "INSERT INTO accounts (accid, name, balance, branchid, address) VALUES("+i+", 'abcabcabcabcbacbacb', 0, 1, 'bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr');";
			stmt = con.createStatement();
			int v = stmt.executeUpdate(SQL);
			stmt.close();
		}
		for(int i = 1; i < n*10; i++)
		{
			String SQL = "INSERT INTO tellers (tellerid, tellername, balance, branchid, address) VALUES("+i+",'abcbaccbaacbbcaabca', 0, 1, 'bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr');";
			stmt = con.createStatement();
	        int v = stmt.executeUpdate(SQL);
	        //rs.close();
	        stmt.close();
		}
		System.out.print("fertig");
		
	}
	catch(Exception e){
		e.printStackTrace();
	}
	finally{
		
		if (con != null) try { con.close(); } catch(Exception e) {}
	}
	}
}
