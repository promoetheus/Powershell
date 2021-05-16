set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "SQS01WPAGL05" 
$databaseName = "Monitoring"

## initialise a class to better manage database backups as objects
. .\ServerInstances.ps1


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

function getAGStatus ($instance) 
{
  #Write-Host "Instance: $instance.name"
  $query_ag_status = "SELECT ag.Name,r.replica_server_name,rs.synchronization_health_desc,rs.role_desc from sys.dm_hadr_availability_replica_states rs 
                         join sys.availability_groups ag on ag.group_id = rs.group_id  join sys.availability_replicas r on r.replica_id = rs.replica_id 
                         where rs.synchronization_health <> 2 ";

  $ags = invoke-sqlcmd -Query  $query_ag_status -Server $instance -Database "master"
  
  foreach ($ag in $ags) {
    $instchk = new-object ServerInstancesStatus
    $instchk.AGName = $ag.Name
    $instchk.ServerName = $ag.replica_server_name
    $instchk.HealthStatus = $ag.synchronization_health_desc
    $instchk.Role = $ag.role_desc

    [ServerInstancesStatus[]]$serverinstancestatus += $instchk
  }
  return $serverinstancestatus
}

$Connection = New-Object System.Data.SQLClient.SQLConnection
$Connection.ConnectionString ="Server=$serverName;Database=$databaseName;trusted_connection=true;"
$Connection.Open()

$Command = New-Object System.Data.SQLClient.SQLCommand
$Command.Connection = $Connection
$Command.CommandText = "TRUNCATE TABLE [dbo].[DBA_AG_Status]"
$Command.ExecuteNonQuery() | out-null
#Write-Host "No. $($agstats.length)"

$instances = invoke-sqlcmd -Query "SELECT B.Server as name FROM [dbo].[DBA_ServerInfoFiles] A INNER JOIN [dbo].DBA_Server_List  B ON B.Server LIKE A.SERVERNAME + '%' WHERE HAType = 'AG' AND BackupCheck = 1" -ServerInstance $serverName -Database $databasename

$agstats = @()
foreach ($instance in $instances ) 
{
    #Write-Host "Instance: $($instance.name)"   
    $agstats += getAGStatus ($instance).name;
}


If ($($agstats.length) -gt 0) 
{
    $agstats[0..($agstats.length-1)] | foreach { 
    $_ | select-object AGName,ServerName,HealthStatus,Role
    #Write-Output "    AG : $($_.ServerName)"
        if ($($_.ServerName))
        {
            $Command.CommandText = "INSERT INTO [dbo].[DBA_AG_Status]([ServerName],[AGName],[Role],[Status]) VALUES('$($_.ServerName)','$($_.AGName)','$($_.Role)','$($_.HealthStatus)')"
            $Command.ExecuteNonQuery() | out-null
        }
    }
}

##HCG Environment
$instances = invoke-sqlcmd -Query "SELECT A.ServerName as name FROM [dbo].[DBA_ServerInfoFiles] A WHERE IsHCG = 'True' --and servername like '%PST01%'" -ServerInstance $serverName -Database $databasename

$agstats = @()
#Write-Output "Instance length: $($instances.length)"
foreach ($instance in $instances ) 
{
    #Write-Output "Instance: $($instance.name)"
    $agstats += getAGStatus ($($instance.name));
    #Write-Output "No. $($agstats.length)"
}


If ($($agstats.length) -gt 0) 
{
    $agstats[0..($agstats.length-1)] | foreach { 
    $_ | select-object AGName,ServerName,HealthStatus,Role
    #Write-Output "    AG : $($_.ServerName)"
        if ($($_.ServerName))
        {
            $Command.CommandText = "INSERT INTO [dbo].[DBA_AG_Status]([ServerName],[AGName],[Role],[Status]) VALUES('$($_.ServerName)','$($_.AGName)','$($_.Role)','$($_.HealthStatus)')"
            $Command.ExecuteNonQuery() | out-null
        }
    }
}

     
Remove-Variable agstats
$Connection.Close()
