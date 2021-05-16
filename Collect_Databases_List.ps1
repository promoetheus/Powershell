set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "UKDC1-PM-SQC01" 
$databaseName = "DBA_CONFIG"


## initialise a file with some variables containing queries (to offload the script)
##. .\queries.ps1


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


function getDatabaseBackups ([String] $instance) {

  #Write-Output "    Instance: $instance"

  $query_bck_database = "select name as DatabaseName FROM master.sys.databases 
        WHERE name NOT IN('master','tempdb','model','msdb') AND state = 0 AND source_database_id IS NULL";


#Write-Host "SQL: $query_bck_database"

  $databases = invoke-sqlcmd -Query  $query_bck_database -Server $instance

  
  $i = 0
  foreach ( $database in $databases ) {
    $dbbck = new-object DatabaseBackup
    $dbbck.instanceName = $instance
    $dbbck.databaseName = $database.DatabaseName

    [DatabaseBackup[]]$databasebackups += $dbbck
  }
  return $databasebackups

}


$Connection = New-Object System.Data.SQLClient.SQLConnection
$Connection.ConnectionString ="Server=$serverName;Database=$databaseName;trusted_connection=true;"
$Connection.Open()

$Command = New-Object System.Data.SQLClient.SQLCommand
$Command.Connection = $Connection

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server] from dbo.DBA_Server_List WHERE DBGROWTHCHECK=1" -ServerInstance $serverName -Database $databasename

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


   $Command.CommandText = "MERGE dbo.DBA_Databases as target USING (
        Select '$($instance.ID)','$($_.instanceName )','$($_.databaseName )')
            as source (InstanceID,InstanceName, DatabaseName)
        ON (source.InstanceName  = target.ServerName and source.DatabaseName=target.DatabaseName)
        WHEN NOT MATCHED THEN
            INSERT (ServerID,ServerName, DatabaseName)
            VALUES ('$($instance.ID)', source.InstanceName, source.DatabaseName);"
    # $Command.CommandText
    $Command.ExecuteNonQuery() | out-null

  }

  Remove-Variable databasebackups

}

# Remove the Server Instances if removed
$command.commandtext = "Delete from dbo.DBA_Databases where not exists(select 'x' from dba_server_list where DBA_Databases.ServerID = dba_server_list.ID AND dba_server_list.DBGROWTHCHECK=1)"
$command.executenonquery() | out-null

$Command.CommandText = "DELETE FROM dbo.DBA_Databases WHERE NOT EXISTS(SELECT 'X' from #DataSource X WHERE DBA_Databases.ServerName = X.InstanceName and DBA_Databases.DatabaseName = X.DatabaseName)"
$Command.ExecuteNonQuery() | out-null

##$Connection.Close()


$servers = invoke-sqlcmd -Query "Select ID = [ServerID], [ServerName],[DatabaseName] from dbo.DBA_Databases WHERE ServerName <> '[UKDC1-PM-SQC01]'" -ServerInstance $serverName -Database $databasename
#$servers = invoke-sqlcmd -Query "Select ID = [ServerID], [ServerName],[DatabaseName] from dbo.DBA_Databases WHERE ServerName LIKE '%WPSISDB05%'" -ServerInstance $serverName -Database $databasename

foreach ($server in $servers) 
{
    $serverdbs  = invoke-sqlcmd -Query "SELECT GETDATE() AS SnapShotDate,'$($server.ID)' AS ServerID, @@SERVERNAME AS ServerName, B.database_id AS DatabaseID,
                '$($server.DatabaseName)' AS DatabaseName, B.file_id As file_Id,B.type_desc AS file_type_desc,B.state_desc AS state_desc,B.growth,B.is_percent_growth,
                A.name AS LogicalFileName, A.physical_name AS PhysicalFileName, A.size/128.0 AS CurrentSizeMB, 
                CAST(FILEPROPERTY(A.name, 'SpaceUsed') AS INT)/128.0 AS UsedSpaceMB,
                A.size/128.0 - CAST(FILEPROPERTY(A.name, 'SpaceUsed') AS INT)/128.0 AS FreeSpaceMB 
                FROM sys.database_files A INNER JOIN master.sys.master_files B ON A.physical_name collate DATABASE_DEFAULT= B.physical_name collate DATABASE_DEFAULT 
                AND A.[file_id] = B.[file_id];" -ServerInstance $server.ServerName -Database $server.Databasename
    
    foreach ($serverdb in $serverdbs)
    {
        $Command.CommandText = "INSERT INTO DBA_CONFIG.dbo.DBA_DatabaseSizes
                    (SnapShotDate,ServerID,ServerName,DatabaseID,DatabaseName,[file_id],file_type_desc,state_desc,growth,is_percent_growth,LogicalFileName,PhysicalFileName,FileSizeMB,UsedSizeMB,UnallocatedSpaceMB) 
                    SELECT '$($serverdb.SnapShotDate)',$($serverdb.ServerID),'$($serverdb.ServerName)',$($serverdb.DatabaseID),'$($serverdb.DatabaseName)',$($serverdb.file_Id),'$($serverdb.file_type_desc)','$($serverdb.state_desc)',$($serverdb.growth),'$($serverdb.is_percent_growth)','$($serverdb.LogicalFileName)','$($serverdb.PhysicalFileName)',$($serverdb.CurrentSizeMB),$($serverdb.UsedSpaceMB),$($serverdb.FreeSpaceMB)"
        $Command.ExecuteNonQuery() | out-null
        #Write-Host "SQL: $($Command.CommandText)"
    }
}

$Connection.Close()
