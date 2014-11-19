package P7;

import java.sql.*;

public class P7A1a {
	public static long time;
	public static long sTime, eTime;
	public static void main(String[] args)
	{
		time = 0;
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
			
			sTime = System.nanoTime();
			con = DriverManager.getConnection(connectionURL);
			eTime = System.nanoTime();
			time += eTime - sTime;
			
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
				sTime = System.nanoTime();
				stmt = con.createStatement();
		        int v = stmt.executeUpdate(SQL);
		        //rs.close();
		        stmt.close();

				eTime = System.nanoTime();
				time += eTime - sTime;
			}
			for(int i = 1; i < n*100000; i++)
			{
				String SQL = "INSERT INTO accounts (accid, name, balance, branchid, address) VALUES("+i+", 'abcabcabcabcbacbacb', 0, 1, 'bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr');";
				
				sTime = System.nanoTime();
				stmt = con.createStatement();
				int v = stmt.executeUpdate(SQL);
				stmt.close();

				eTime = System.nanoTime();
				time += eTime - sTime;
			}
			for(int i = 1; i < n*10; i++)
			{
				String SQL = "INSERT INTO tellers (tellerid, tellername, balance, branchid, address) VALUES("+i+",'abcbaccbaacbbcaabca', 0, 1, 'bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr');";
				
				sTime = System.nanoTime();
				stmt = con.createStatement();
		        int v = stmt.executeUpdate(SQL);
		        //rs.close();
		        stmt.close();
				eTime = System.nanoTime();
				time += eTime - sTime;
			}
			System.out.print("fertig");
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			
			if (con != null) 
				try 
				{
					sTime = System.nanoTime(); 
					con.close();
					eTime = System.nanoTime();
					time += eTime - sTime;
				} catch(Exception e) {}
			System.out.println("Zeit[ms]: "+ time / (float)1000000);
		}
	}
}
