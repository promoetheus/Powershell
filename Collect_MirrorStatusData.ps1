set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "UKDC1-PM-SQC01" 
$databaseName = "DBA_CONFIG"


$Current = Get-Location

## add required snap-in to query sqlserver
if ( Get-PSSnapin -Registered | where {$_.name -eq 'SqlServerCmdletSnapin100'} )
{
	if( !(Get-PSSnapin | where {$_.name -eq 'SqlServerCmdletSnapin100'}))
	{ 
		Add-PSSnapin SqlServerCmdletSnapin100 | Out-Null
	} ;
}
else
{
	if( !(Get-Module | where {$_.name -eq 'sqlps'}))
	{ 
		Import-Module 'sqlps' -DisableNameChecking ;
	}  ;
}

Set-Location $Current


$Connection = New-Object System.Data.SQLClient.SQLConnection
$Connection.ConnectionString ="Server=$serverName;Database=$databaseName;trusted_connection=true;"
$Connection.Open()

$Command = New-Object System.Data.SQLClient.SQLCommand
$Command.Connection = $Connection

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server] from dbo.DBA_Server_List WHERE MIRRORCHECK=1 ORDER BY [server]" -ServerInstance $serverName -Database $databasename

foreach ( $instance in $instances ) 
{
    # Get the list of mirror databases for the instance
    #Write-Output "INSTANCE :      $($instance.name)"

    $mirrordbquery = "SELECT [mirrordbname]=A.name FROM sys.databases A INNER JOIN sys.database_mirroring B 
            ON A.database_id=B.database_id 
            WHERE a.database_id > 4 AND B.mirroring_state is NOT NULL
            ORDER BY A.NAME"

    $mirrordbs = invoke-sqlcmd -Query $mirrordbquery -ServerInstance $instance.name #-Database $databasename
    foreach($mirrordb in $mirrordbs)
    {
        #Write-Output "Database Name :      $($mirrordb.mirrordbname)"

        $mirrormonitorquery = "DECLARE	@MonitorResults AS TABLE (
        database_name VARCHAR(255),
        role INT,
        mirror_state TINYINT,
        witness_status TINYINT,
        log_generat_rate INT,
        unsent_log INT,
        sent_rate INT,
        unrestored_log INT,
        recovery_rate INT,
        transaction_delay INT,
        transaction_per_sec INT,
        average_delay INT,
        time_recorded DATETIME,
        time_behind DATETIME,
        local_time DATETIME);
 
        INSERT INTO @MonitorResults
                       EXEC sp_dbmmonitorresults 
					        @database_name = '$($mirrordb.mirrordbname)',
					        @mode = 1
        
        SELECT *, DATEDIFF(S,time_behind, time_recorded) AS Latency from @MonitorResults"
    

        $mirrorresults = invoke-sqlcmd -Query $mirrormonitorquery -ServerInstance $instance.name -Database msdb
    
        foreach($mirrordata in $mirrorresults)
        {
            #Write-Output "Database Name :      $($mirrordata.database_name)"
            $Command.CommandText = "MERGE dbo.DBA_global_mirror_monitor as target USING (
                Select '$($instance.name)', '$($mirrordata.database_name)',$($mirrordata.role),$($mirrordata.mirror_state),$($mirrordata.witness_status),$($mirrordata.log_generat_rate),
                 $($mirrordata.unsent_log), $($mirrordata.sent_rate),$($mirrordata.unrestored_log),$($mirrordata.recovery_rate),$($mirrordata.transaction_delay),$($mirrordata.transaction_per_sec),
                 $($mirrordata.average_delay),'$($mirrordata.time_recorded)','$($mirrordata.time_behind)','$($mirrordata.local_time)',$($mirrordata.latency)
                ) AS source (Server_name, DatabaseName, mirror_role, mirror_status, witness_status, log_generation_rate, unsent_log, sent_rate, unrestored_log, recovery_rate,
                    transaction_delay, transactions_per_sec, average_delay, time_recorded, time_behind, Collection_Datetime,latency)
        ON (source.Server_name  = target.Server_name and source.DatabaseName=target.Databasename And source.Collection_Datetime = target.Collection_Datetime)
         WHEN MATCHED THEN
                UPDATE SET [mirror_role] = source.mirror_role,[mirror_status]=source.mirror_status,[witness_status]=source.witness_status,[sent_rate]=source.sent_rate,
                    [transaction_delay]=source.transaction_delay, [transactions_per_sec]=source.transactions_per_sec,[log_generation_rate]=source.log_generation_rate,
                    [unsent_log]=source.unsent_log, [unrestored_log]=source.unrestored_log,[recovery_rate]=source.recovery_rate, [average_delay]=source.average_delay,
                    [time_recorded]=source.time_recorded, [time_behind]=source.time_behind,[Latency]=source.latency
        WHEN NOT MATCHED THEN
            INSERT ([Server_name],[DatabaseName],[mirror_role],[mirror_status],[witness_status],[sent_rate],[transaction_delay],[transactions_per_sec],[log_generation_rate],[unsent_log],
                [unrestored_log],[recovery_rate],[average_delay],[time_recorded],[time_behind],[Latency],[Collection_Datetime])
            VALUES (source.Server_name, source.DatabaseName, source.mirror_role, source.mirror_status, source.witness_status, source.sent_rate, source.transaction_delay, 
                    source.transactions_per_sec, source.log_generation_rate, source.unsent_log, source.unrestored_log, source.recovery_rate, source.average_delay, 
                    source.time_recorded, source.time_behind, source.Latency, source.Collection_Datetime);"
        #Write-Output "$($Command.CommandText)"
        # $Command.CommandText
        $Command.ExecuteNonQuery() | out-null

        }
    }
}

$Command.CommandText = "Delete from dbo.DBA_global_mirror_monitor WHERE Collection_Datetime < DateAdd(d,-7,getdate())"
$Command.ExecuteNonQuery() | out-null

$Connection.Close()
