set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "SQS01WPAGL05" 
$databaseName = "Monitoring"


## initialise a class to better manage database backups as objects
. .\Databaserowcounts3.ps1


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



$Connection1 = New-Object System.Data.SQLClient.SQLConnection
$Connection1.ConnectionString ="Server=$instance;Database=master;trusted_connection=true;"
$Connection1.Open()

$Command1 = New-Object System.Data.SQLClient.SQLCommand
$Command1.Connection = $Connection1


 $Command1.CommandText = "SET NOCOUNT ON
DECLARE 

@cmd varchar(8000)



DROP TABLE IF EXISTS ##databases
DROP TABLE IF EXISTS tempdb.dbo.rowcounts

SELECT 
name 
INTO ##databases
FROM sys.databases
WHERE name IN ('PaymentTrust','STLink','DMS','PaymentTrust_NW','PaymentTrustRefundArc','PTReportsLive','3DSecure','TUBI','Pandora3','StoreHouse3')



DECLARE @databasename varchar(100)
,@ServerName varchar(100)

DECLARE DatabaseNeedsDeletes CURSOR FOR   
SELECT name 
FROM ##Databases

OPEN DatabaseNeedsDeletes

FETCH NEXT FROM DatabaseNeedsDeletes   
INTO @databasename

WHILE @@FETCH_STATUS = 0  
BEGIN 

SET @Servername = '[' +@@SERVERNAME +']'
	
	set @cmd ='
	CREATE TABLE ##results(Servername varchar(100),DatabaseName varchar(100),name varchar(100),rows bigint,DateGathered datetime)    

    INSERT INTO ##results
    SELECT DISTINCT '''+ @servername +''' AS Servername,'''+ @databasename +''' AS DatabaseName,so.name ,convert(bigint, (select sum(p2.rows)
            from ['+ @databasename +'].sys.indexes i2 inner join['+ @databasename +'].sys.partitions p2 ON i2.object_id = p2.OBJECT_ID AND i2.index_id = p2.index_id
            where i2.object_id = so.object_id and i2.object_id > 255 and (i2.index_id = 0 or i2.index_id = 1)
        ) ) AS rows, convert(date,GETDATE()) AS DateGathered 
	--INTO ##results
	   FROM ['+ @databasename +'].sys.objects so
       JOIN  ['+ @databasename +'].sys.partitions sp ON so.object_id = sp.object_id
       WHERE  so.is_ms_shipped !=1
       AND rows >500000
       AND Index_id IN (0,1)
	   --AND Partition_number !>1
       ORDER BY name'
	    
	
	EXEC(@cmd)

	IF OBJECT_ID('tempdb.dbo.rowcounts') IS NOT NULL
	BEGIN 

		INSERT 
		INTO tempdb.dbo.rowcounts
		SELECT * FROM ##results
	END
	ELSE
	BEGIN 
		SELECT *
		INTO tempdb.dbo.rowcounts
		FROM ##results
	END 

	DROP TABLE IF EXISTS ##results

 
    FETCH NEXT FROM DatabaseNeedsDeletes   
    INTO @Databasename

END
       
CLOSE DatabaseNeedsDeletes  
DEALLOCATE DatabaseNeedsDeletes"
   $Command1.ExecuteNonQuery() | out-null






        $query_bck_database = "SELECT DISTINCT Servername AS ServerName,DatabaseName AS DatabaseName,name AS name,convert(bigint,rows) AS rows ,convert(varchar(12),DateGathered) AS DateGathered FROM tempdb.dbo.rowcounts";
    }
  
  
#Write-Host "SQL: $query_bck_database"

  $databases = invoke-sqlcmd -Query  $query_bck_database -Server $instance -QueryTimeout 120

  
  $i = 0
  foreach ( $database in $databases ) {
    $dbbck = new-object RowCount
    $dbbck.instanceName = $instance
    $dbbck.databaseName = $database.DatabaseName
    $dbbck.name = $database.name
    $dbbck.rows = $database.rows
    $dbbck.DateGathered = $database.DateGathered
    

    [RowCount[]]$databasebackups += $dbbck
  }
  return $databasebackups

}


$Connection = New-Object System.Data.SQLClient.SQLConnection
$Connection.ConnectionString ="Server=$serverName;Database=$databaseName;trusted_connection=true;"
$Connection.Open()

$Command = New-Object System.Data.SQLClient.SQLCommand
$Command.Connection = $Connection

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server] from dbo.DBA_HCG_Server_List_ListenerNames" -ServerInstance $serverName -Database $databasename

$Command.CommandText = "IF OBJECT_ID('tempdb..#DataSource') IS NOT NULL 
        DROP TABLE #DataSource"
$Command.ExecuteNonQuery() | out-null

$Command.CommandText = "CREATE TABLE #DataSource
(
    InstanceName  varchar(128) NOT NULL,
    DatabaseName varchar(128) NOT NULL
);"
$Command.ExecuteNonQuery() | out-null


foreach ( $instance in $instances ) {

  $databasebackups = getDatabaseBackups ($instance.name);
  # $databasebackups

  $databasebackups[0..($databasebackups.length-1)] | foreach {

	$_ | select-object instanceName,databaseName


   $Command.CommandText = "INSERT INTO #DataSource(InstanceName,DatabaseName) VALUES('$($_.instanceName )','$($_.databaseName )')"
   $Command.ExecuteNonQuery() | out-null

    $Command.CommandText = "INSERT INTO dbo.DBA_HCG_RowCounts VALUES ('$($_.instanceName  )','$($_.DatabaseName )','$($_.name )','$($_.rows )','$($_.DateGathered )')" 
    # $Command.CommandText
    $Command.ExecuteNonQuery() | out-null

  }


  Remove-Variable databasebackups

}

$Command.CommandText = "DELETE FROM dbo.DBA_HCG_RowCounts WHERE DateGathered <= DATEADD(d,-180,getutcdate())"
$Command.ExecuteNonQuery() | out-null

$Connection.Close()
