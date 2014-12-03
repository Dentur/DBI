

package p9;

import java.sql.*;
import java.util.Random;

public class LoadDriver {
	long warmupTime;
	long lagTime;
	long measureTime;
	long cooldownTime; 
	
	int verKonto;
	int verEinzahlung;
	int verAnalyse;
	
	Connection connection;
	
	public long actions;
	/**
	 * 
	 * @param conString String to Connect to the Database
	 * @param warmupT	Time in ms to Execute Statements without measuring
	 * @param lagT		Time in ms to Wait after each Stament
	 * @param measureT	Time in ms to measure tps
	 * @param cooldownT	Time in ms to keep running after the measurement
	 * @param verKonto	Wahrscheinlichkeit das ein KontoStatement ausgewählt wird (verKonto in 100)
	 * @param verEinzahlung Wahrscheinlichkeit das ein EinzahlungsStatement ausgewählt wird (verEinzahlung in 100)
	 * @param verAnalyse Wahrscheinlichkeit das ein AnalyseStatement ausgewählt wird (verAnalyse in 100)
	 * @throws SQLException
	 */
	public LoadDriver(String conString, long warmupT, long lagT, long measureT, long cooldownT, int verKonto, int verEinzahlung, int verAnalyse ) throws SQLException
	{
		try
		{
			connection = DriverManager.getConnection(conString);
		}
		catch(SQLException e)
		{
			throw e;
		}
		
		if(warmupT >= 0)
			warmupTime = warmupT;
		else
			warmupTime = 0;
		
		if(lagT >= 0)
			lagTime = lagT;
		else
			lagTime = 0;
		
		if(measureT >= 0)
			measureTime = measureT;
		else
			measureTime = 0;
		
		if(cooldownT >= 0)
			cooldownTime = cooldownT;
		else
			cooldownTime = 0;
		
		if(verKonto >= 0)
			this.verKonto = verKonto;
		else
			this.verEinzahlung = 0;
		
		if(verEinzahlung >= 0)
			this.verEinzahlung = verEinzahlung;
		else
			this.verEinzahlung = 0;
		
		if(verAnalyse >= 0)
			this.verAnalyse = verAnalyse;
		else
			this.verAnalyse = 0;
	}
	
	/*
	 * Starts the Insertloop
	 */
	public long start()
	{
		long anzahl = 0, runtime = 0, startTime, tempTime;
		int phase = 0;//0 = warmup; 1 = measure; 3 = cooldown;
		boolean run = true;
		startTime = System.nanoTime();
		Random rand = new Random();
		int auswahl;
		while(run)
		{
			tempTime = System.nanoTime();
			runtime = (long) ((tempTime - startTime)/(float)1000000);
			if(runtime < warmupTime)
			{
				phase = 0;
			}
			else if(runtime > measureTime + warmupTime)
			{
				phase = 2;
			}
			else if(runtime > (measureTime + warmupTime + cooldownTime))
			{
				run = false;
			}
			else
			{
				phase = 1;
			}
			
			//Statement absetzen
			auswahl = rand.nextInt(100) +1;
			if( auswahl <= verKonto)
			{
				System.out.println("Konto");
			}
			else if((auswahl > verKonto) && (auswahl < (verKonto + verEinzahlung)) )
			{
				System.out.println("Einzahlung");
			}
			else if(auswahl > (verKonto + verEinzahlung) && (auswahl < (verKonto + verEinzahlung + verAnalyse))) 
			{
				System.out.println("Analyse");
			}
			
			
			if(phase == 1)
				anzahl++;
			
			
			try {
				wait(lagTime);
			} catch (InterruptedException e) {
				//Interupt exeption
				//mache erstmal nichts
			}
		}
		
		return anzahl;
	}
	
	public double kontostand_tx(Connection cn ,int kd_id)
	{
		Statement st = null;
		ResultSet rs = null;
		st = cn.createStatement();
		rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + kd_id);
		return rs.getDouble(0);
	}
	
	public double einzahlung_tx(Connection cn, int kd_id, int tl_id, int br_id, double delta)
	{
		Statement st = null;
		ResultSet rs = null;
		
		st = cn.createStatement();
		rs = st.executeUpdate("UPDATE branches SET balance = balance + " + delta + " WHERE branchid = " + br_id);
		
		return rs.getDouble(0);
	}

}
