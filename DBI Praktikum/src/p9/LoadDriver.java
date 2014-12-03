

package p9;

import java.sql.*;
import java.util.Random;

public class LoadDriver {
	long warmupTime;
	long lagTime;
	long measureTime;
	long cooldownTime;
	
	Connection connection;
	
	public long actions;
	
	public LoadDriver(String conString, long warmupT, long lagT, long measureT, long cooldownT) throws SQLException
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
	}
	
	public long start()
	{
		long anzahl = 0, runntime = 0, startTime, tempTime;
		int phase = 0;//0 = warmup; 1 = measure; 3 = cooldown;
		boolean run = true;
		startTime = System.nanoTime();
		while(run)
		{
			tempTime = System.nanoTime();
			runntime = (long) ((tempTime - startTime)/(float)1000000);
			if(runntime < warmupTime)
			{
				phase = 0;
			}
			else if(runntime > measureTime + warmupTime)
			{
				phase = 2;
			}
			else
			{
				phase = 1;
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

}
