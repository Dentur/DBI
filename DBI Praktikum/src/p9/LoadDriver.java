

package p9;

import java.sql.*;
import java.util.Random;

public class LoadDriver extends Thread {
	long warmupTime;
	long lagTime;
	long measureTime;
	long cooldownTime; 
	
	int verKonto;
	int verEinzahlung;
	int verAnalyse;
	
	int threadNr;
	Connection connection;
	
	public int phase;
	
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
	public LoadDriver(String conString, long warmupT, long lagT, long measureT, long cooldownT, int verKonto, int verEinzahlung, int verAnalyse, int threadNr ) throws SQLException
	{
	this.threadNr = threadNr;	
		try
		{
			connection = DriverManager.getConnection(conString);
			connection.setTransactionIsolation(connection.TRANSACTION_SERIALIZABLE);
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
	public void run()
	{

		System.out.println("LoadDriver Started");
		long runtime = 0, startTime, tempTime;
		phase = 0;//0 = warmup; 1 = measure; 2 = cooldown; 3 = Beenden
		boolean run = true;
		startTime = System.nanoTime();
		Random rand = new Random();
		int auswahl;
		actions = 0;
		while(run)
		{
			if(phase == 3)
				run = false;
			//Statement absetzen
			auswahl = rand.nextInt(100) +1;
			if( auswahl <= verKonto)
			{
				//System.out.println("Konto");
				this.kontostand_tx(connection, rand.nextInt(10000000));
			}
			else if((auswahl > verKonto) && (auswahl < (verKonto + verEinzahlung)) )
			{
				//System.out.println("einzahlung");
				this.einzahlung_tx(connection, rand.nextInt(10000000), rand.nextInt(1000), rand.nextInt(100), rand.nextInt());
			}
			else if(auswahl > (verKonto + verEinzahlung) && (auswahl < (verKonto + verEinzahlung + verAnalyse))) 
			{
				//System.out.println("Analyse");
				this.analyse_tx(connection, rand.nextDouble());
			}
			
			
			if(phase == 1)
				actions++;
			
			
			try {
				sleep(lagTime);
			} catch (InterruptedException e) {
				//Interupt exeption
				//mache erstmal nichts
			}
		}
		System.out.println("Thread finished with : " + actions +" Querys");
	}
	
	public double kontostand_tx(Connection cn ,int kd_id)
	{
		Statement st = null;
		ResultSet rs = null;
		double erg = 0;
		try {
			st = cn.createStatement();
			rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + kd_id + ";");
			erg = rs.getInt(0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("konto");
			e.printStackTrace();
		}
		
		return erg;
	}
	
	public double einzahlung_tx(Connection cn, int kd_id, int tl_id, int br_id, int delta)
	{
		Statement st = null;
		ResultSet rs = null;
		double erg = 0;
		try {
			st = cn.createStatement();
			st.executeUpdate("UPDATE branches SET balance = balance + " + delta + " WHERE branchid = " + br_id + ";");
			
			st.executeUpdate("UPDATE tellers SET balance = balance + " + delta + " WHERE tellerid = " + tl_id + ";");
			
			st.executeUpdate("UPDATE accounts SET balance = balance + " + delta + " WHERE accid = " + kd_id + ";");
			
			rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + kd_id + ";");
			st.executeQuery("INSERT INTO history (accid, tellerid, delta, branchid, accbalance) VALUES ("+ kd_id + "," + tl_id + "," + delta + "," + br_id + "," + rs.getDouble(0) + ");");
			
			erg = rs.getInt(0);
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("einzahlung");
			e.printStackTrace();
		}
		
		return erg;
	}

	public int analyse_tx(Connection cn, double delta)
	{
		Statement st = null;
		ResultSet rs = null;
		int anz = 0;
		try {
			st = cn.createStatement();
			rs = st.executeQuery("SELECT COUNT(delta) AS Anzahl FROM history WHERE delta = " + delta + ";");
			anz = rs.getInt(0);
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("analyse");
			e.printStackTrace();
		}
		return anz;
	}
}
