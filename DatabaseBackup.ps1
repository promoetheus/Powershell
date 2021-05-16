Add-Type @'

using System;

public class DatabaseBackup
{
    public string instanceName;
    public int instanceID;
    public string databaseName;
    public string recoveryMode;
    public string status;
    public DateTime creationTime;
    public DateTime lastFull;
    public DateTime lastDiff;
    public DateTime lastLog;


    
    private TimeSpan diff;
	
    public double lastFullTotalHours () {
		diff = DateTime.Now - lastFull;
		return Math.Round(diff.TotalHours,2);
    }

    public double lastDiffTotalHours () {
		diff = DateTime.Now - lastDiff;
		return Math.Round(diff.TotalHours,2);
    }

    public double lastLogTotalHours () {
		diff = DateTime.Now - lastLog;
		return Math.Round(diff.TotalHours,2);
    }

}
'@    
