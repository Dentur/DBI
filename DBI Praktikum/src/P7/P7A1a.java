package P7;

import java.sql.*;
import java.util.Random;

public class P7A1a {
	public static long time, time2;
	public static long sTime2, eTime2;
	public static int batchSize = 1000;
	public static void main(String[] args)
	{
		Random rand = new Random();
		//Erstelle den verbindungs String
		String connectionURL = "jdbc:sqlserver://localhost:1433;"
		+ "databaseName=BenchmarkDB;user=test;password=123";
		
		//JDBC Objects
		Connection con = null;
		
		try {
			PreparedStatement pst;
			//Lade den msjdbc treiber
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			//Verbiindung mit dem Server herstellen
			con = DriverManager.getConnection(connectionURL);
			
			int ns[] = {100, 20, 50};
			for(int index = 0; index < 1; index++)
			{
				//Alte Datensätze löschen
				String SQLL = "DELETE FROM tellers;DELETE FROM accounts;DELETE FROM branches;";
				Statement st = con.createStatement();
				int l = st.executeUpdate(SQLL);
				st.close();
				
				//Start eines Durchlaufs
				
				sTime2 = System.nanoTime();
				//Deaktiviere auto Commit 
				con.setAutoCommit(false);
				int n = ns[index];
				//Erstelle das insert Statement für die branches
				pst = con.prepareStatement("INSERT INTO branches (branchid, branchname, balance, address) "+
						"VALUES (?,?,?,?);");
				//Branches mit Tupeln füllen
				for(int i = 0; i < n; i++)
				{
					//Setze Werte
					pst.setInt(1, i);//Branchid
					pst.setString(2,"bcerabcerabcerabcer");//Branchname
					pst.setInt(3,0);//balance
					pst.setString(4, "bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabr");//address
					//Füge das Tupel dem Batch hinzu
					pst.addBatch();
					//Führe den Batch aus wenn die batchSize erreicht wurde 
					if((i % batchSize) == 0)
					{
						pst.executeBatch();
					}
						
				}
				System.out.print("branches fertig \n");
				//Führe den Batch mit den rest der Tupel aus und beende die Transaction
				pst.executeBatch();
				con.commit();
				//Das Prepared Statement wird geschlossen
				pst.close();
				//Erstelle das Insert Statement für die account
				pst = con.prepareStatement("INSERT INTO accounts (accid, name, balance, branchid, address) "+
						"VALUES (?,?,?,?,?);");
				//Fülle accounts mit Tupeln
				for(int i = 0; i < n*100000; i++)
				{
					//Setze Werte
					pst.setInt(1, i);//accid
					pst.setString(2,"abcabcabcabcbacbacb");//name
					pst.setInt(3,0);//balance
					pst.setInt(4, rand.nextInt(n));//branchid
					pst.setString(5, "bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr");//address
					//Füge das Tupel dem Batch hinzu
					pst.addBatch();
					//Führe den Batch aus wenn die batchSize erreicht wurde 
					if((i % batchSize) == 0)
					{
						pst.executeBatch();
					}
				}
				System.out.print("accounts fertig \n");
				//Führe den Batch mit den rest der Tupel aus und beende die Transaction
				pst.executeBatch();
				con.commit();
				pst.close();
				//Erstelle das Insert Statement der teller
				pst = con.prepareStatement("INSERT INTO tellers (tellerid, tellername, balance, branchid, address) "+
						"VALUES (?,?,?,?,?);");
				//Fülle Teller mit Tupeln
				for(int i = 0; i < n*10; i++)
				{
					//Setze Werte
					pst.setInt(1, i);//Tellerid
					pst.setString(2,"abcbaccbaacbbcaabca");//tellername
					pst.setInt(3,0);//balance
					pst.setInt(4, rand.nextInt(n));//branchid
					pst.setString(5, "bcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcerabcr");//address
					//Füge das Tupel dem Batch hinzu
					pst.addBatch();
					//Führe den Batch aus wenn die batchSize erreicht wurde 
					if((i % batchSize) == 0)
					{
						pst.executeBatch();
					}
				}
				System.out.print("tellers fertig \n");
				//Führe den Batch mit den rest der Tupel aus und beende die Transaction
				pst.executeBatch();
				con.commit();
				pst.close();
				
				System.out.print("fertig");
				//Messen der Zeit nach abschluss des befüllens
				eTime2 = System.nanoTime();
				//Ausgabe der Zeit
				System.out.println("Zeit[ms] gesanntProgramm bei n: + "+n+": "+ (eTime2 - sTime2) / (float)1000000);
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		//Schließe die verbindung egal was passiert
		finally{
			
			if (con != null) 
				try 
				{ 
					//Verbindung wird geschlossen
					con.close();
				} catch(Exception e) {}
			
			
		}
		
	}
}
