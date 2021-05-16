set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "SQS01WPAGL05" 
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
		#First check if the server is none UTC
        $query_bck_database = "if datediff(MINUTE,GETDATE(),GETUTCDATE()) > 0
					begin
						SELECT [Database] = d.NAME
							,dateadd(hour,-1,LastFull) as LastFull
							,dateadd(hour,-1,LastDIff) as LastDIff
							,dateadd(hour,-1,LastTran) as LastTran
							,GetDate = getdate()
							,RecoveryMode = DATABASEPROPERTYEX(d.NAME, 'Recovery')
							,CreationTime = d.create_date
							,STATUS = DATABASEPROPERTYEX(d.NAME, 'Status')
                            ,ReadOnly = d.is_read_only

						FROM master.sys.databases d
						LEFT OUTER JOIN (
							SELECT database_name
								,LastFull = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE (type = 'D')
								AND backup_finish_date <= getdate()
							GROUP BY database_name
							) b ON d.NAME = b.database_name
						LEFT OUTER JOIN (
							SELECT database_name
								,LastTran = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE type = 'L'
								AND backup_finish_date <= getdate()
							GROUP BY database_name
							) c ON d.NAME = c.database_name
						LEFT OUTER JOIN (
							SELECT database_name
								,LastDiff = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE type = 'I'
								AND backup_finish_date <= getdate()
							GROUP BY database_name
							) e ON d.NAME = e.database_name
						WHERE d.NAME <> 'Tempdb'
							AND sys.fn_hadr_backup_is_preferred_replica(d.NAME) = 1
							AND d.source_database_id IS NULL
						ORDER BY [LastFull]
					end
					else
					begin
						SELECT [Database] = d.NAME
							,LastFull
							,LastDiff
							,LastTran
							,GetDate = getutcdate()
							,RecoveryMode = DATABASEPROPERTYEX(d.NAME, 'Recovery')
							,CreationTime = d.create_date
							,STATUS = DATABASEPROPERTYEX(d.NAME, 'Status')
                            ,ReadOnly = d.is_read_only

						FROM master.sys.databases d
						LEFT OUTER JOIN (
							SELECT database_name
								,LastFull = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE (type = 'D')
								AND backup_finish_date <= getutcdate()
							GROUP BY database_name
							) b ON d.NAME = b.database_name
						LEFT OUTER JOIN (
							SELECT database_name
								,LastTran = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE type = 'L'
								AND backup_finish_date <= getutcdate()
							GROUP BY database_name
							) c ON d.NAME = c.database_name
						LEFT OUTER JOIN (
							SELECT database_name
								,LastDiff = max(backup_finish_date)
							FROM msdb.dbo.backupset
							WHERE type = 'I'
								AND backup_finish_date <= getutcdate()
							GROUP BY database_name
							) e ON d.NAME = e.database_name
						WHERE d.NAME <> 'Tempdb'
							AND sys.fn_hadr_backup_is_preferred_replica(d.NAME) = 1
							AND d.source_database_id IS NULL
						ORDER BY [LastFull]
					end
						";
    }
  
  
#Write-Host "SQL: $query_bck_database"

  $databases = invoke-sqlcmd -Query  $query_bck_database -Server $instance -QueryTimeout 120

  
  $i = 0
  foreach ( $database in $databases ) {
    $dbbck = new-object DatabaseBackup
    $dbbck.instanceName = $instance
    $dbbck.databaseName = $database.Database
    $dbbck.recoveryMode = $database.RecoveryMode
    $dbbck.creationTime = $database.CreationTime
    $dbbck.status = $database.Status
    $dbbck.ReadOnly = $database.ReadOnly

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

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server],[Backup2Share] from dbo.DBA_HCG_Server_List WHERE BACKUPCHECK=1" -ServerInstance $serverName -Database $databasename

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
  select '$($instance.ID)','$($_.instanceName )','$($_.databaseName )','$($_.recoveryMode )','$($_.status )','$($_.creationTime)','$($_.lastFull)','$($_.lastDiff)','$($_.lastLog)','$($_.ReadOnly)')
  as source (InstanceID,InstanceName, DatabaseName, RecoveryMode, DatabaseStatus, CreationTime, LastFull, LastDiff, LastLog, ReadOnly)
ON (source.InstanceID = target.InstanceID and source.InstanceName = target.InstanceName and source.DatabaseName=target.DatabaseName 
    AND CAST(target.ProcessExecutionTime as DATE) = CAST(GETUTCDATE() AS Date))
 WHEN MATCHED THEN
  UPDATE SET RecoveryMode = source.RecoveryMode, DatabaseStatus = source.DatabaseStatus, CreationTime = source.CreationTime,
   LastFull = source.LastFull, LastDIff = source.LastDiff, LastLog = source.LastLog, ProcessExecutionTime=getutcdate(),Backup2Share='$($instance.Backup2Share)'
   ,IsReadOnly = source.ReadOnly
 WHEN NOT MATCHED THEN
  INSERT (InstanceID,InstanceName, DatabaseName, RecoveryMode, DatabaseStatus, CreationTime, LastFull, LastDiff, LastLog, Backup2Share, ProcessExecutionTime, IsHCG, IsReadOnly)
   VALUES ('$($instance.ID)', source.InstanceName, source.DatabaseName,source.RecoveryMode,source.DatabaseStatus, source.CreationTime, source.LastFull,source.LastDiff,source.LastLog
   ,'$($instance.Backup2Share)', getutcdate(), 'True', source.ReadOnly );
"
    # $Command.CommandText
    $Command.ExecuteNonQuery() | out-null

  }


  Remove-Variable databasebackups

}

$Command.CommandText = "DELETE FROM DBA_Backup_Status WHERE ProcessExecutionTime <= DATEADD(d,-90,getutcdate())"
$Command.ExecuteNonQuery() | out-null

$Connection.Close()
