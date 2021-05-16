set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "UKDC1-PM-SQS01A" 
$databaseName = "Monitoring"


## initialise a class to better manage database backups as objects
. .\DatabaseBackup.ps1


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


function getDatabaseBackups ([String]$instance) {

  #Write-Output "    Instance: $instance"

  $ver = Invoke-Sqlcmd -Query "SELECT Version = CONVERT(INT,SUBSTRING(CAST(serverproperty('ProductVersion') AS nvarchar), 1, CHARINDEX('.', CAST(serverproperty('ProductVersion') AS nvarchar)) - 1))" -Server $instance
  
  foreach ( $Versionrecord in $ver ) 
  {
    $versionNo = $Versionrecord.Version
  }
  
  #Write-Host "Version No: $versionNo"
  
  if ($versionNo -gt 10)
    {
        $query_bck_database = "select [Database]=d.name,LastFull,LastDiff,LastTran,GetDate=getdate(), RecoveryMode=DATABASEPROPERTYEX(d.name, 'Recovery'),
            CreationTime=d.create_date, Status=DATABASEPROPERTYEX(d.name, 'Status')
            from master.sys.databases d
            left outer join
            (select database_name, LastFull=max(backup_finish_date)
                from msdb.dbo.backupset
                where (type = 'D') and backup_finish_date <= getdate()
                group by database_name
            ) b
            on d.name = b.database_name
            left outer join
            (select database_name, LastTran=max(backup_finish_date)
                from msdb.dbo.backupset
                where type ='L' and backup_finish_date <= getdate()
                group by database_name
            ) c
            on d.name = c.database_name
            left outer join
            (select database_name, LastDiff=max(backup_finish_date)
                from msdb.dbo.backupset
                where type ='I' and backup_finish_date <= getdate()
                group by database_name
            ) e
            on d.name = e.database_name
            where d.name <> 'Tempdb' AND sys.fn_hadr_backup_is_preferred_replica( d.name) = 1 AND d.source_database_id IS NULL
            order by [LastFull]";
    }
    else
    {
        $query_bck_database = "select [Database]=d.name,LastFull,LastDiff,LastTran,GetDate=getdate(), RecoveryMode=DATABASEPROPERTYEX(d.name, 'Recovery'),
            CreationTime=d.create_date, Status=DATABASEPROPERTYEX(d.name, 'Status')
            from master.sys.databases d
            left outer join
            (select database_name, LastFull=max(backup_finish_date)
                from msdb.dbo.backupset
                where (type = 'D') and backup_finish_date <= getdate()
                group by database_name
            ) b
            on d.name = b.database_name
            left outer join
            (select database_name, LastTran=max(backup_finish_date)
                from msdb.dbo.backupset
                where type ='L' and backup_finish_date <= getdate()
                group by database_name
            ) c
            on d.name = c.database_name
            left outer join
            (select database_name, LastDiff=max(backup_finish_date)
                from msdb.dbo.backupset
                where type ='I' and backup_finish_date <= getdate()
                group by database_name
            ) e
            on d.name = e.database_name
            where d.name <> 'Tempdb' AND d.source_database_id IS NULL
            order by [LastFull]";
    }

#Write-Host "SQL: $query_bck_database"

  $databases = invoke-sqlcmd -Query  $query_bck_database -Server $instance

  
  $i = 0
  foreach ( $database in $databases ) {
    $dbbck = new-object DatabaseBackup
    $dbbck.instanceName = $instance
    $dbbck.databaseName = $database.Database
    $dbbck.recoveryMode = $database.RecoveryMode
    $dbbck.creationTime = $database.CreationTime
    $dbbck.status = $database.Status
    if ( -not ( $database.IsNull("LastFull") ) ) {
      $dbbck.lastFull = $database.LastFull
    } else {
      $dbbck.lastFull = "01.01.1900 00:00:00"
    }

    if ( -not ( $database.IsNull("LastDiff") ) ) {
      $dbbck.lastDiff = $database.LastDiff
    } else {
      $dbbck.lastDiff = "01.01.1900 00:00:00"
    }

    if ( -not ( $database.IsNull("LastTran") ) ) {
      $dbbck.lastLog = $database.LastTran
    } else {
      $dbbck.lastLog = "01.01.1900 00:00:00"
    }

    [DatabaseBackup[]]$databasebackups += $dbbck
  }
  return $databasebackups

}


$Connection = New-Object System.Data.SQLClient.SQLConnection
$Connection.ConnectionString ="Server=$serverName;Database=$databaseName;trusted_connection=true;"
$Connection.Open()

$Command = New-Object System.Data.SQLClient.SQLCommand
$Command.Connection = $Connection

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server],[Backup2Share] from dbo.DBA_Server_List WHERE BACKUPCHECK=1" -ServerInstance $serverName -Database $databasename

$Command.CommandText = "IF OBJECT_ID('tempdb..#DataSource') IS NOT NULL 
        DROP TABLE #DataSource"
$Command.ExecuteNonQuery() | out-null

$Command.CommandText = "CREATE TABLE #DataSource
(
    InstanceName  varchar(128) NOT NULL,
    DatabaseName varchar(128) NOT NULL
    PRIMARY KEY (InstanceName,DatabaseName)
);"
$Command.ExecuteNonQuery() | out-null


foreach ( $instance in $instances ) {

  $databasebackups = getDatabaseBackups ($instance.name);
  # $databasebackups

  $databasebackups[0..($databasebackups.length-1)] | foreach {

	$_ | select-object instanceName,databaseName


   $Command.CommandText = "INSERT INTO #DataSource(InstanceName,DatabaseName) VALUES('$($_.instanceName )','$($_.databaseName )')"
   $Command.ExecuteNonQuery() | out-null


   $Command.CommandText = "MERGE dbo.DBA_Backup_Status as target USING (
  select '$($instance.ID)','$($_.instanceName )','$($_.databaseName )','$($_.recoveryMode )','$($_.status )','$($_.creationTime)','$($_.lastFull)','$($_.lastDiff)','$($_.lastLog)')
  as source (InstanceID,InstanceName, DatabaseName, RecoveryMode, DatabaseStatus, CreationTime, LastFull, LastDiff, LastLog)
ON (source.InstanceID = target.InstanceID and source.DatabaseName=target.DatabaseName AND CAST(target.ProcessExecutionTime as DATE) = CAST(GETDATE() AS Date))
 WHEN MATCHED THEN
  UPDATE SET RecoveryMode = source.RecoveryMode, DatabaseStatus = source.DatabaseStatus, CreationTime = source.CreationTime,
   LastFull = source.LastFull, LastDIff = source.LastDiff, LastLog = source.LastLog, ProcessExecutionTime=getdate(),Backup2Share='$($instance.Backup2Share)'
 WHEN NOT MATCHED THEN
  INSERT (InstanceID,InstanceName, DatabaseName, RecoveryMode, DatabaseStatus, CreationTime, LastFull, LastDiff, LastLog, Backup2Share, ProcessExecutionTime)
   VALUES ('$($instance.ID)', source.InstanceName, source.DatabaseName,source.RecoveryMode,source.DatabaseStatus, source.CreationTime, source.LastFull,source.LastDiff,source.LastLog,'$($instance.Backup2Share)', getdate() );
"
    # $Command.CommandText
    $Command.ExecuteNonQuery() | out-null

  }


  Remove-Variable databasebackups

}


# Remove the records if any instance removed
$command.commandtext = "Delete from dba_backup_status where not exists(select 'x' from dba_server_list where dba_backup_status.instancename = dba_server_list.server AND dba_server_list.BACKUPCHECK=1) AND (CAST(ProcessExecutionTime as DATE) = CAST(Getdate() AS Date))"
$command.executenonquery() | out-null

$Command.CommandText = "DELETE FROM DBA_Backup_Status WHERE NOT EXISTS(SELECT 'X' from #DataSource X WHERE DBA_Backup_Status.InstanceName = X.InstanceName and DBA_Backup_Status.DatabaseName = X.DatabaseName) AND (CAST(ProcessExecutionTime as DATE) = CAST(Getdate() AS Date))"
$Command.ExecuteNonQuery() | out-null

$Command.CommandText = "DELETE FROM DBA_Backup_Status WHERE ProcessExecutionTime <= DATEADD(d,-90,getdate())"
$Command.ExecuteNonQuery() | out-null

$Connection.Close()