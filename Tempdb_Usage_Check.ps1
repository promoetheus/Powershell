set-location "E:\MSSQL\Powershell\"

### environment to insert results/get lists
$serverName = "UKDC1-PM-SQC01" 
$databaseName = "DBA_CONFIG"
$threshold = 85

## initialise a file with some variables containing queries (to offload the script)
##. .\queries.ps1


## initialise a class to better manage database backups as objects
##. .\DatabaseBackup.ps1


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

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server] from dbo.DBA_Server_List WHERE TempdbCheck=1" -ServerInstance $serverName -Database $databasename

$Command.CommandText = "TRUNCATE TABLE DBA_CONFIG.dbo.DBA_TempDBSizes"
$Command.ExecuteNonQuery() | out-null

##$Connection.Close()

foreach ($instance in $instances) 
{
    #Write-Output  $instance.Name

    $serverdbs = Invoke-Sqlcmd -Query "SELECT GETDATE() AS SnapShotDate, '$($instance.ID)' AS ServerID, @@SERVERNAME AS ServerName, 
                A.fileid As file_Id,CASE A.groupid WHEN 0 THEN 'LOG' ELSE 'PRIMARY' END AS file_type_desc, 
                B.growth, B.is_percent_growth,  A.name as LogicalFileName, A.filename As PhysicalFileName,
                A.size/128.0 AS FileSizeMB, CAST(FILEPROPERTY(A.name, 'SpaceUsed') AS INT)/128.0 AS UsedSizeMB,
                100-(100 * (CAST (((A.size/128.0 - CAST(FILEPROPERTY(A.name,  'spaceused') AS int)/128.0)/(A.size/128.0)) AS decimal(4,2)))) AS UsedSpacePerc 
	    FROM sys.sysfiles A INNER JOIN master.sys.master_files B ON A.filename collate DATABASE_DEFAULT= B.physical_name collate DATABASE_DEFAULT 
	    AND  A.[fileid] = B.[file_id];" -ServerInstance $instance.Name -Database tempdb

    foreach ($serverdb in $serverdbs)
    {
        $Command.CommandText = "INSERT INTO DBA_CONFIG.dbo.DBA_TempDBSizes
                    (SnapShotDate,ServerID,ServerName,[file_id],file_type_desc,growth,is_percent_growth,LogicalFileName,PhysicalFileName,FileSizeMB,UsedSizeMB,UsedSpacePerc) 
                    SELECT '$($serverdb.SnapShotDate)',$($serverdb.ServerID),'$($serverdb.ServerName)',$($serverdb.file_Id),'$($serverdb.file_type_desc)',$($serverdb.growth),
                    '$($serverdb.is_percent_growth)','$($serverdb.LogicalFileName)','$($serverdb.PhysicalFileName)',$($serverdb.FileSizeMB),$($serverdb.UsedSizeMB),$($serverdb.UsedSpacePerc)"
        $Command.ExecuteNonQuery() | out-null
        #Write-Host "SQL: $($Command.CommandText)"
    }
}

#---------------------------------------------------------------------------------------------

$a = "<style>"
#$a = $a + "BODY{background-color:peachpuff;}"
$a = $a + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
$a = $a + "TR{border-width: 1px;padding: 1px;border-style: solid;border-color: black;;background-color:thistle}"
$a = $a + "TD{border-width: 1px;padding: 0px;border-style: solid;border-color: black;}"
$a = $a + "</style>"

$query = "SELECT SnapShotDate,ServerName,file_type_desc,LogicalFileName,PhysicalFileName,FileSizeMB,UsedSizeMB,UsedSpacePerc
    FROM DBA_CONFIG.dbo.DBA_TempDBSizes WHERE UsedSpacePerc > $threshold "
    #Write-Output "$query"
$thresholdExceeddata = invoke-sqlcmd -Query $query -ServerInstance $serverName -Database $databasename

foreach ($data in $thresholdExceeddata)
{
    $ArrayList+= "<tr><td>" + "$($data.Snapshotdate)" + "</td><td>" + "$($data.ServerName)" + "</td><td>" + "$($data.file_type_desc)" + "</td>"
    $ArrayList+= "<td>" + "$($data.LogicalFileName)" + "</td><td>" + "$($data.PhysicalFileName)" + "</td>"
    $ArrayList+= "<td>" + "$($data.FileSizeMB)" + "</td><td>" + "$($data.UsedSizeMB)" + "</td><td>" + "$($data.UsedSpacePerc)" + "</td></tr>"
    #Write-Output "Entered"
        
}

#Write-Output "Length is " $ArrayList.Length
if ($ArrayList.Length -gt 0 )
{
    $header = "<TR><td><b>Collection Date</b></td><td><b>Server Name</b></td><td><b>File Type</b></td><td><b>Logical Name</b></td><td><b>File Path</b></td><td><b>File Size(MB)</b></td><td><b>Used Size(mb)</b></td><td><b>Used Space%</b></td></TR>"
    $outputlist = "<table>" + $header
    foreach ($y in $ArrayList)
    {
        #Write-Output $y
        $outputlist += $y 
    }
    $outputlist += "</table>"
    #Write-Output $outputlist
    ##**************************************
    ##Final Code to Combine all fragments
    ##**************************************

    $OutputFile_new = "E:\MSSQL\Powershell\tempdb.html"

    ConvertTo-HTML -head $a -PostContent $outputlist -PreContent "<h1>SQL Server Tempdb File Space Usage Report</h1>" | Out-File $OutputFile_new

    $smtpServer = "mail.worldpay.local"
    $anonUsername = "anonymous"
    $anonPassword = ConvertTo-SecureString -String "anonymous" -AsPlainText -Force
    $anonCredentials = New-Object System.Management.Automation.PSCredential($anonUsername,$anonPassword)

    $attachment = "$OutputFile_new"
    $body= Get-Content $OutputFile_new
    $subject = "SQL Server Tempdb Space Usage Exceed threshold of " + $threshold + "% Report "

    #write-host "Sending email"

    Send-MailMessage -to "rsc_dba@worldpay.com" -from "noreply@SQLServerDBMail.worldpay.com" -SmtpServer "mail.worldpay.local" -subject $subject -credential $anonCredentials -BodyAsHtml "$body" #-Attachments $attachment #$OutputFile_new

}


$Connection.Close()