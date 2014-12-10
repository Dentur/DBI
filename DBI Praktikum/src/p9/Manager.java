package p9;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Manager {
	//Parameter
	int AnzahlLoadDriver = 5;
	String VerbindungsaufbauString = "jdbc:sqlserver://localhost:1433; databaseName=BenchmarkDB;user=test;password=123";
	long warmupTime = 240*1000;
	long measureTime = 300*1000;
	long cooldownTime = 60*1000;
	long thinkTime = 50;
	int verteilungKontoStatement = 35;
	int verteilungEinzahlungsStatement = 50;
	int verteilungAnalyseStatement = 15;
	//Variablen
	LoadDriver[] drivers;
	Connection connection;
	
	public Manager()
	{
		if(false)
		{
			warmupTime = 1000;
			measureTime = 6*warmupTime;
			cooldownTime = warmupTime;
		}
		try
		{
			connection = DriverManager.getConnection(VerbindungsaufbauString);
			Statement st = connection.createStatement();
			st.executeUpdate("Delete From History;");
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		
	}
	
	public void createLoadDriver()
	{
		drivers = new LoadDriver[AnzahlLoadDriver];
		System.out.println("Starte Unterprogramme:");
		for(int i = 0; i < AnzahlLoadDriver; i++)
		{
			try {
				drivers[i] = new LoadDriver(VerbindungsaufbauString, warmupTime, thinkTime, measureTime, cooldownTime, verteilungKontoStatement, this.verteilungEinzahlungsStatement, this.verteilungAnalyseStatement, i );
				drivers[i].start();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		try {
			
			Thread.sleep(warmupTime);
			System.out.println("Warmup Finished");
			for(int i = 0; i < AnzahlLoadDriver; i++)
			{
				drivers[i].phase = 1;
			}
			Thread.sleep(this.measureTime);
			System.out.println("MeasureTime Finished");
			for(int i = 0; i < AnzahlLoadDriver; i++)
			{
				drivers[i].phase = 2;
			}
			Thread.sleep(this.cooldownTime);
			System.out.println("Cooldown Finished");
			for(int i = 0; i < AnzahlLoadDriver; i++)
			{
				drivers[i].phase = 3;
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
