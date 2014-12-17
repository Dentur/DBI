

package p9;

import java.sql.*;
import java.util.Random;

/**
 * Apllies load to a SQL Server
 * @author Sebastian Venhuis
 *
 */
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
	
	PreparedStatement pstKt;
	PreparedStatement pstAl;
	PreparedStatement pstEinzBraBalance;
	PreparedStatement pstEinzUpBranch;
	PreparedStatement pstEinzUpTellers;
	PreparedStatement pstEinzUpAccounts;
	PreparedStatement pstEinzAccBalance;
	PreparedStatement pstEinzInsHistory;
	
	/**
	 * The phase of the loadDriver
	 */
	public int phase;
	
	/**
	 * The actions the Loaddriver has exequted
	 */
	public long actions;
	/**
	 * 
	 * @param conString String to Connect to the Database
	 * @param warmupT	Time in ms to Execute Statements without measuring
	 * @param lagT		Time in ms to Wait after each Stament
	 * @param measureT	Time in ms to measure tps
	 * @param cooldownT	Time in ms to keep running after the measurement
	 * @param verKonto	Wahrscheinlichkeit das ein KontoStatement ausgew�hlt wird (verKonto in 100)
	 * @param verEinzahlung Wahrscheinlichkeit das ein EinzahlungsStatement ausgew�hlt wird (verEinzahlung in 100)
	 * @param verAnalyse Wahrscheinlichkeit das ein AnalyseStatement ausgew�hlt wird (verAnalyse in 100)
	 * @throws SQLException
	 */
	public LoadDriver(String conString, long lagT, int verKonto, int verEinzahlung, int verAnalyse, int threadNr ) throws SQLException
	{
	this.threadNr = threadNr;	
		try
		{
			connection = DriverManager.getConnection(conString);
			connection.setTransactionIsolation(connection.TRANSACTION_SERIALIZABLE);
			connection.setAutoCommit(false);
			pstKt = connection.prepareStatement("SELECT balance FROM accounts WHERE accid = ?;");
			pstAl = connection.prepareStatement("SELECT COUNT(delta) AS Anzahl FROM history WHERE delta = ?;");
			pstEinzBraBalance = connection.prepareStatement("SELECT balance FROM branches WHERE branchid = ?;");
			pstEinzUpBranch = connection.prepareStatement("UPDATE branches SET balance = ? WHERE branchid = ?;");
			pstEinzUpTellers = connection.prepareStatement("UPDATE tellers SET balance = ? WHERE tellerid = ?;");
			pstEinzUpAccounts = connection.prepareStatement("UPDATE accounts SET balance = ? WHERE accid = ?;");
			pstEinzAccBalance = connection.prepareStatement("SELECT balance FROM accounts WHERE accid = ?;");
			pstEinzInsHistory = connection.prepareStatement("INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES (?,?,?,?,?,?);");
		}
		catch(SQLException e)
		{
			throw e;
		}
		
		//Set all parameters		
		if(lagT >= 0)
			lagTime = lagT;
		else
			lagTime = 0;
		
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
	
	/**
	 * Starts the Execution of the Loaddriver
	 * To Call use thread.start()
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
			//Beende wenn die endphase erreicht wurde
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
					this.analyse_tx(connection, rand.nextInt());
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
	/**
	 * Get the balance of a account
	 * @param cn the Connnection to the Database
	 * @param kd_id the id of the account
	 * @return the balance of the selected account
	 */
	public int kontostand_tx(Connection cn ,int kd_id)
	{
		Statement st = null;
		ResultSet rs = null;
		int erg = 0;
		try {
			pstKt.setInt(1, kd_id);
			rs = pstKt.executeQuery();
			rs.next();
			erg = rs.getInt(1);
		} catch (SQLException e) {
			this.actions--;
			// TODO Auto-generated catch block
			System.out.println("konto");
			e.printStackTrace();
		}
		
		return erg;
	}
	
	/**
	 * �bt die einzahlung aus
	 * @param cn Die Verbindung zu der Datenbank
	 * @param kd_id Der Kunde der Geld einzahlt
	 * @param tl_id	Der Bankautomat an dem einbezahlt wird
	 * @param br_id Die Fiale in der Einbezahlt wurde
	 * @param delta	Der geldbetrag der eingezahlt wurde
	 * @return Der neue Geldbetrag von dem Kunden
	 */
	public int einzahlung_tx(Connection cn, int kd_id, int tl_id, int br_id, int delta)
	{
		Statement st = null;
		ResultSet rs = null;
		int erg = 0;
		try {
			pstEinzBraBalance.setInt(1, br_id);
			rs = this.pstEinzBraBalance.executeQuery();
			rs.next();
			
			delta = delta + rs.getInt(1);
			rs.close();
			
			this.pstEinzUpBranch.setInt(1, delta);
			this.pstEinzUpBranch.setInt(2, br_id);
			this.pstEinzUpBranch.executeUpdate();
			
			this.pstEinzUpTellers.setInt(1, delta);
			this.pstEinzUpTellers.setInt(2, tl_id);
			this.pstEinzUpTellers.executeUpdate();

			this.pstEinzUpAccounts.setInt(1, delta);
			this.pstEinzUpAccounts.setInt(2, kd_id);
			this.pstEinzUpAccounts.executeUpdate();
			
			this.pstEinzAccBalance.setInt(1, kd_id);
			ResultSet rss = this.pstEinzAccBalance.executeQuery();
			rss.next();
			erg = rss.getInt(1);
			
			this.pstEinzInsHistory.setInt(1, kd_id);
			this.pstEinzInsHistory.setInt(2, tl_id);
			this.pstEinzInsHistory.setInt(3, delta);
			this.pstEinzInsHistory.setInt(4, br_id);
			this.pstEinzInsHistory.setInt(5, erg);
			this.pstEinzInsHistory.setString(6, "abcdeabcdeabcdeabcdeabcdeabcd");
			this.pstEinzInsHistory.executeUpdate();
			//rss.close();
		} catch (SQLException e) {
			this.actions--;
			try {
				
				connection.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// TODO Auto-generated catch block
			//System.out.println("einzahlung");
			//e.printStackTrace();
		}
		
		return erg;
	}

	/**
	 * Gibt die anzahl der einzahlungen von einem bestimten �berweisungsbetrag an
	 * @param cn Die Verbinduung zum Server
	 * @param delta Der gew�nschte Geldbetrag
	 */
	public int analyse_tx(Connection cn, int delta)
	{
		Statement st = null;
		ResultSet rs = null;
		int anz = 0;
		try {
			pstAl.setInt(1, delta);
			rs = pstAl.executeQuery();
			rs.next();
			anz = rs.getInt(1);
			rs.close();
		} catch (SQLException e) {
			this.actions--;
			// TODO Auto-generated catch block
			System.out.println("analyse");
			e.printStackTrace();
		}
		return anz;
	}
}
