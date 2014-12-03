package p9;

import java.sql.SQLException;

public class Manager {
	//Parameter
	int AnzahlLoadDriver = 2;
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
	
	public Manager()
	{
		
	}
	
	public void createLoadDriver()
	{
		drivers = new LoadDriver[AnzahlLoadDriver];
		System.out.println("Starte Unterprogramme:");
		for(int i = 0; i < AnzahlLoadDriver; i++)
		{
			try {
				drivers[i] = new LoadDriver(VerbindungsaufbauString, warmupTime, thinkTime, measureTime, cooldownTime, verteilungKontoStatement, this.verteilungEinzahlungsStatement, this.verteilungAnalyseStatement );
				drivers[i].start();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
}
