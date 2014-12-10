

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
			connection.setAutoCommit(false);
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
		Random rand = new Random((long)this.threadNr);
		int auswahl;
		actions = 0;
		while(run)
		{
			if(phase == 3)
				run = false;
			//Statement absetzen
			auswahl = rand.nextInt(100) +1;
			try
			{
				if( auswahl <= verKonto)
				{
					//System.out.println("Konto");
					this.kontostand_tx(connection, rand.nextInt(10000000));
					connection.commit();
				}
				else if((auswahl > verKonto) && (auswahl < (verKonto + verEinzahlung)) )
				{
					//System.out.println("einzahlung");
					this.einzahlung_tx(connection, rand.nextInt(10000000), rand.nextInt(1000), rand.nextInt(100), rand.nextInt());
					
					connection.commit();
				}
				else if(auswahl > (verKonto + verEinzahlung) && (auswahl < (verKonto + verEinzahlung + verAnalyse))) 
				{
					//System.out.println("Analyse");
					this.analyse_tx(connection, rand.nextDouble());
					connection.commit();
				}
			}
			catch(SQLException e)
			{
				
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
	
	public int kontostand_tx(Connection cn ,int kd_id)
	{
		Statement st = null;
		ResultSet rs = null;
		int erg = 0;
		try {
			st = cn.createStatement();
			rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + kd_id + ";");
			rs.next();
			erg = rs.getInt(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("konto");
			e.printStackTrace();
		}
		
		return erg;
	}
	
	public int einzahlung_tx(Connection cn, int kd_id, int tl_id, int br_id, int delta)
	{
		Statement st = null;
		ResultSet rs = null;
		int erg = 0;
		try {
			st = cn.createStatement();
			rs = st.executeQuery("SELECT balance FROM branches WHERE branchid = " + br_id + ";");
			rs.next();
			delta = delta + rs.getInt(1);
			st.executeUpdate("UPDATE branches SET balance = " + delta + " WHERE branchid = " + br_id + ";");
			
			st.executeUpdate("UPDATE tellers SET balance = " + delta + " WHERE tellerid = " + tl_id + ";");
			
			st.executeUpdate("UPDATE accounts SET balance = " + delta + " WHERE accid = " + kd_id + ";");
			rs.close();
			ResultSet rss = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + kd_id + ";");
			rss.next();

			erg = rss.getInt(1);
			st.executeUpdate("INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES ("+ kd_id + "," + tl_id + "," + delta + "," + br_id + "," + erg + ", + 'abcdeabcdeabcdeabcdeabcdeabcd');");
			
			//rss.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//System.out.println("einzahlung");
			//e.printStackTrace();
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
			rs.next();
			anz = rs.getInt(1);
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("analyse");
			e.printStackTrace();
		}
		return anz;
	}
}
