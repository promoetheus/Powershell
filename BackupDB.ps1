
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

$s = New-Object ('Microsoft.SqlServer.Management.Smo.Server') "UKDC1-PM-SQC01" 

#Create a Backup object instance with the Microsoft.SqlServer.Management.Smo.Backup namespace 
$dbBackup = new-object ("Microsoft.SqlServer.Management.Smo.Backup") 

#Set the Database property to Northwind 
$dbBackup.Database = "DBA_CONFIG" 

#Add the backup file to the Devices collection and specify File as the backup type 
$dbBackup.Devices.AddDevice("E:\Temp\DBA_Config_FULL.bak", "File")

#Specify the Action property to generate a FULL backup 
$dbBackup.Action="Database"

#Call the SqlBackup method to generate the backup 
$dbBackup.SqlBackup($s)
