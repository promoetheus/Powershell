$style = @"
<style>
body {
    color:#333333;
    font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
    font-size: 11px;
}
h1 {
    border-top:1px solid #666666;
    text-align:left;
    font-size: 18px;
}

h2 {
    border-top:1px solid #666666;
    font-size: 16px;
}


table {
    border-width: 1px;
    border-style: solid;
    border-color: white;
    border-collapse: collapse;
    border: none;
    padding: 1px;
    }

td {
    border-width: 1px;
    padding: 1px;
    border-style: solid;
    border-color: white;
    }

th {
    font-weight:bold;
    color:black;
    background-color:#94D4F9;
    cursor:pointer;
    border-width: 1px;
    padding: 1px;
    border-style: solid;
    border-color: white
}


.odd  { background-color:#E5E5E5; }
.even { background-color:#D5EDFA; }
.paginate_enabled_next, .paginate_enabled_previous {
    cursor:pointer; 
    border:1px solid #222222; 
    background-color:#dddddd; 
    padding:2px; 
    margin:4px;
    border-radius:2px;
}
.paginate_disabled_previous, .paginate_disabled_next {
    color:#666666; 
    cursor:pointer;
    background-color:#dddddd; 
    padding:2px; 
    margin:4px;
    border-radius:2px;
}
.dataTables_info { margin-bottom:4px; }
.sectionheader { cursor:pointer; }
.sectionheader:hover { color:red; }
.grid { width:dynamic }
.red {
    color:red;

} 
</style>
"@

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
#. $PSScriptRoot\SQLETLFunctions.ps1
. $PSScriptRoot\EnhancedHTML2.ps1

$serverName = "UKDC1-PM-SQC01" 
$databaseName = "DBA_CONFIG"

#####################################################
# Email function
#####################################################
function Send-EmailToSupport
{
	param (
		[Parameter(Mandatory = $true)][string]$emailBody,
		[Parameter(Mandatory = $true)][string]$emailSubject
	)
	
	$EmailFrom = "noreply@SQLServerDBMail.worldpay.com"
	$EmailTo = "rsc_dba@worldpay.com"

	$SMTPServer = "mail.worldpay.local"
	
	$mailer = new-object Net.Mail.SMTPclient($smtpserver)
	$msg = new-object Net.Mail.MailMessage($EmailFrom, $EmailTo, $EmailSubject, $Emailbody)
	$msg.IsBodyHTML = $true
	$mailer.send($msg)
} # end of function


#####################################################
# Blocking function
#####################################################
function Get-SQLBlocking([string] $Server, [int]$DurationToReport)
{
	$ESCkey = 27 # 27 is the key number for the Esc button.
	$database = "master"

    $date = Get-Date
    $filename = "{0}-{1:d2}-{2:d2}_{3:d2}-{4:d2}" -f $date.year, $date.month, $date.day, $date.hour, $date.minute, $date.second
    $ReportFile = "$PSScriptRoot\SQLBlocking_$filename.html"

    [string]$CurrentTime = Get-Date -format "hh:mm:ss tt"
    #$TimeDiff = New-TimeSpan -Start $CurrentTime -End $RunUntil

	$Tableparams = @{
		'As' = 'Table';
		'PreContent' = '<br/> <b>&diams; SQL Blocking Details</b>';
		'EvenRowCssClass' = 'even';
		'OddRowCssClass' = 'odd';
		'TableCssClass' = 'grid'
		'Properties' = #@{n='Start Time';e={$_.blocking_start_time}},
		
		@{ n = 'Start Time'; e = { "{0:hh\:mm\:ss\ tt}" -f ($_.blocking_start_time) } },
		@{ n = 'Blocked'; e = { $_.blocked_spid } },
		@{ n = 'Blocking'; e = { $_.blocking_spid }; css = { if ($_.blocking_spid -ge 0) { 'red' } } },
		#@{n='Duration (ms)';e={$_.blocking_duration_ms}},
		@{ n = 'Duration'; e = { ("{0:hh\:mm\:ss\,fff}" -f [timespan]::fromseconds($_.blocking_duration_ms /1000)) } },
		@{ n = 'Blocked Command'; e = { $_.BlockedReqCommand } },
		@{ n = 'Blocked Waiting for'; e = { $_.wait_type } },
		@{ n = 'Blocked Resource'; e = { $_.blocked_resource } },
		
		@{ n = 'Blocking App'; e = { $_.blocking_app } },
		@{ n = 'Blocking DB'; e = { $_.blocking_db } },
		@{ n = 'Blocking Host'; e = { $_.blocking_host } },
		#@{ n = 'Blocking OS User'; e = { $_.blocking_os_user } },
		@{ n = 'Blocking User'; e = { $_.blocking_db_user } },
		@{ n = 'Blocking Batch/SP'; e = { $_.blocking_sql_text } },
		@{ n = 'Blocking Statement'; e = { $_.BlockingStmt } },
		
		@{ n = 'Blocked App'; e = { $_.blocked_app } },
		@{ n = 'Blocked DB'; e = { $_.blocked_db } },
		@{ n = 'Blocked Host'; e = { $_.blocked_host } },
		#@{ n = 'Blocked OS User'; e = { $_.blocked_os_user } },
		@{ n = 'Blocked User'; e = { $_.blocked_db_user } },
		@{ n = 'Blocked Batch/SP'; e = { $_.blocked_sql_text } },
		@{ n = 'Blocked Statement'; e = { $_.BlockedStmt } }
		
	}
	
	
	# blocking query
	$query = "
            set nocount on
set transaction isolation level read uncommitted
select  getdate() as collection_time ,
        wt.session_id 'blocked_spid' ,
        wt.blocking_session_id 'blocking_spid' ,
        Blockedsess.last_request_start_time blocking_start_time ,
        wait_duration_ms 'blocking_duration_ms' ,
        BlockedReq.command 'BlockedReqCommand' ,
        wt.wait_type 'wait_type' ,
        wt.resource_description blocked_resource ,
        isnull(db_name(BlockedReq.database_id), '') 'blocked_db' ,
        BlockedSess.program_name 'blocked_app' ,
        BlockedSess.host_name 'blocked_host' ,
        BlockedSess.nt_user_name 'blocked_os_user' ,
        BlockedSess.login_name 'blocked_db_user' ,
        left(( select top 1
                                text
                        from      sys.dm_exec_sql_text(BlockedConn.most_recent_sql_handle)
                    ), 1024) 'blocked_sql_text' ,
        left(( select top 1
                                text
                        from      sys.dm_exec_sql_text(BlockingConn.most_recent_sql_handle)
                    ), 1024) 'blocking_sql_text' ,
        left((case when BlockedReq.sql_handle is null then 'N/A'
                else substring(( select top 1
                                        text
                                from      sys.dm_exec_sql_text(BlockedConn.most_recent_sql_handle)
                            ), ( BlockedReq.statement_start_offset + 2 ) / 2,
                            ( case when BlockedReq.statement_end_offset = -1
                                    then len(convert(nvarchar(max), ( select top 1
                                                                text
                                                                from
                                                                sys.dm_exec_sql_text(BlockedConn.most_recent_sql_handle)
                                                                ))) * 2
                                    else BlockedReq.statement_end_offset
                                end - BlockedReq.statement_start_offset ) / 2)
        end), 1024) as BlockedStmt ,
        left((case when BlockingReq.sql_handle is null then 'N/A'
                else substring(( select top 1
                                        text
                                from      sys.dm_exec_sql_text(BlockingConn.most_recent_sql_handle)
                            ), ( BlockingReq.statement_start_offset + 2 ) / 2,
                            ( case when BlockingReq.statement_end_offset = -1
                                    then len(convert(nvarchar(max), ( select top 1
                                                                text
                                                                from
                                                                sys.dm_exec_sql_text(BlockingConn.most_recent_sql_handle)
                                                                ))) * 2
                                    else BlockingReq.statement_end_offset
                                end - BlockingReq.statement_start_offset ) / 2)
        end ), 1024) as BlockingStmt ,
        isnull(db_name(BlockingReq.database_id), '') 'blocking_db' ,
        BlockingSess.program_name 'blocking_app' ,
        BlockingSess.host_name 'blocking_host' ,
        BlockingSess.nt_user_name 'blocking_os_user' ,
        BlockingSess.login_name 'blocking_db_user'
from    ( select    session_id ,
                    blocking_session_id ,
                    wait_duration_ms ,
                    wait_type ,
                    resource_description ,
                    row_number() over ( partition by session_id order by wait_duration_ms desc ) rnum
            from      sys.dm_os_waiting_tasks with ( readpast )
            where     blocking_session_id is not null
                    and blocking_session_id <> session_id
                    and blocking_session_id != @@spid
        ) wt
        left join sys.dm_exec_sessions BlockedSess with ( readpast ) on wt.session_id = BlockedSess.session_id
        left join sys.dm_exec_connections BlockedConn with ( readpast ) on wt.session_id = BlockedConn.session_id
        left join sys.dm_exec_requests BlockedReq with ( readpast ) on wt.session_id = BlockedReq.session_id
        left join sys.dm_exec_sessions BlockingSess with ( readpast ) on wt.blocking_session_id = BlockingSess.session_id
        left join sys.dm_exec_connections BlockingConn with ( readpast ) on wt.blocking_session_id = BlockingConn.session_id
        left join sys.dm_exec_requests BlockingReq with ( readpast ) on wt.blocking_session_id = BlockingReq.session_id
where   wt.rnum = 1
order by wait_duration_ms desc"
		
	# database connection
	$connectionTemplate = "Data Source={0};Integrated Security=SSPI;Initial Catalog={1};"
	$connectionString = [string]::Format($connectionTemplate, $server, $database)
	$connection = New-Object System.Data.SqlClient.SqlConnection
	$connection.ConnectionString = $connectionString
	
	$command = New-Object System.Data.SqlClient.SqlCommand
	$command.CommandText = $query
	$command.Connection = $connection
	
	$SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
	$SqlAdapter.SelectCommand = $command
	# $DataSet = New-Object System.Data.DataSet
	# $SqlAdapter.Fill($DataSet)
	# $connection.Close()
	
	
	
		
        [string]$CurrentTime = Get-Date -format "hh:mm:ss tt"
        #$TimeDiff = New-TimeSpan -Start $CurrentTime -End $RunUntil

		# Get new events
		$DataSet = New-Object System.Data.DataSet
        # $DataSet.Clear()		
        [void]$SqlAdapter.Fill($DataSet)
		
		$sql_blocking = @{}
		$sql_blocking = $DataSet.Tables[0] #| select * | Out-DataTable
        
        $blocking_duration = $sql_blocking.blocking_duration_ms		
		$blocking_rowcount = $DataSet.Tables[0].Rows.Count
		
		# If new blocking happens, then retrieve the blocking information.
		
        if ($blocking_rowcount -ge 1 -and $blocking_duration -ge $DurationToReport) 
		{
			
			$html_blocking = $sql_blocking | ConvertTo-EnhancedHTMLFragment @Tableparams
			$root_blocker = $sql_blocking | select -First 1			
			
			$root_blocker_spid = $root_blocker.blocking_spid
			$root_blocker_duration = $root_blocker.blocking_duration_ms
			$root_blocker_blockingStatement = $root_blocker.BlockingStmt
			$root_blocker_blockIngSQLText = $root_blocker.blocking_sql_text
			$root_blocker_blocked_resource = $root_blocker.blocked_resource
			$root_blocker_blocking_app = $root_blocker.blocking_App
			
			$root_blocker_blocked_Database = $root_blocker.blocked_db
			$root_blocker_blocking_User = $root_blocker.blocking_db_user
			$root_blocker_blocking_Host = $root_blocker.blocking_Host
			
			$duration = ("{0:hh\:mm\:ss\,fff}" -f [timespan]::fromseconds($root_blocker_duration/1000))
								
			$Emailparams = @{
				'CssStyleSheet' = $style;
				'Title' = "SQL Blocking on $Server";
				'PreContent' = "<br/> 
                                  <table>
                                        <tr>
                                            <td bgcolor=black> </td>
                                            <td bgcolor=gray> </td>
                                        </tr>    

                                        <tr>
                                            <td bgcolor=#E5E5E5>  <b> SQL blocking occured on </b> </td>
                                            <td bgcolor=#E5E5E5>  <font size=2 color = #0B0B61> <b>$Server </b></font></td>
                                        </tr>    
                                        <tr>
                                            <td  <b> Collection time </b> </td>
                                            <td  <font size=2 color = #0B0B61> $(get-date -Format F) </font> </td>
                                        </tr>

                                        <tr>
                                            <td  <b> Run on </b> </td>
                                            <td  <font size=2 color = #0B0B61> $env:COMPUTERNAME by DBA Team ($env:userdomain\$env:USERNAME) </font> </td>
                                        </tr>

                                        <tr>
                                            <td bgcolor=black> </td>
                                            <td bgcolor=gray></td>
                                        </tr>    

                                        <tr>
                                            <td bgcolor=#E5E5E5>  <b> Total blocked processes </b> </td>
                                            <td bgcolor=#E5E5E5> <font size=2 color = red> <b>$blocking_rowcount </b></font> </td>
                                        </tr>


                                        <tr>
                                            <td>  <b>Duration of blocking</b>  </td>
                                            <td>  <font size=2 color = red> <b>$duration </b> </font> </td>
                                        </tr>    

                                        <tr>
                                            <td bgcolor=#E5E5E5> <b> Head of blocking SPID </b> 
                                            <td bgcolor=#E5E5E5> <font size=2 color = red> <b>$root_blocker_spid </b></font></td>
                                        </tr>    
                                        <tr>
                                            <td><b> Blocked resource </b>  </td>
                                            <td><font size=2 color = red> <b>$root_blocker_blocked_resource </b></font> </td>
                                        </tr>    

                                        <tr>
                                            <td bgcolor=#E5E5E5> <b>Blocked database </b>  </td>
                                            <td bgcolor=#E5E5E5> <font size=2 color = red> <b>$root_blocker_blocked_Database </b></font> </td>
                                        </tr>    

                                        <tr>
                                            <td bgcolor=black> </td>
                                            <td bgcolor=gray></td>
                                        </tr>    
                                        <tr>
                                            <td><b> Blocking Host (User) </b>  </td>
                                            <td> <font size=2 color= #0B0B61> $root_blocker_blocking_host ($root_blocker_blocking_user) </font> </td>
                                        </tr>    


                                        <tr>
                                            <td bgcolor=#E5E5E5> <b> Blocking application </b>   </td>
                                            <td bgcolor=#E5E5E5> <font size=2 color = #0B0B61> $root_blocker_blocking_app </font>  </td>
                                        </tr>    
                                        <tr>
                                            <td bgcolor=black> </td>
                                            <td bgcolor=gray></td>
                                        </tr>    

                                        <tr>
                                            <td> <b> Blocking statement </b> </td>
                                            <td> <font size=2 color=blue> $root_blocker_blockingStatement </font> </td>
                                        </tr>    
                                        <tr>
                                            <td bgcolor=#E5E5E5> <b> Blocking batch/sp </b> </td>
                                            <td bgcolor=#E5E5E5> <font size=2 color = #0B0B61> $root_blocker_blockingSQLText </font> </td>
                                        </tr>    
                                  </table>";
				
				'HTMLFragments' = @($html_blocking)
			}
			
            $Emailbody = ConvertTo-EnhancedHTML @Emailparams  | Out-String
			
			$EmailSubject = "SQL Server Blocking Alert from $Server"
			#Write-Host $emailbody
			#Send Email To support 
			Send-EmailToSupport -EmailBody $Emailbody -EmailSubject $EmailSubject

			$DataSet.Dispose()
			#start-sleep($CheckEverySeconds)
			
		}
		# Check if the Esc key is pressed
		if ($host.ui.RawUi.KeyAvailable)
		{
			$key = $host.ui.RawUI.ReadKey("NoEcho,IncludeKeyUp")
			# If the Esc key is pressed, break the loop, and exit this function .
			if ($key.VirtualKeyCode -eq $ESCkey)
			{
				break
			}
		}
    
	$connection.Close()
}


$query_serverlist = "Select [name]=[server] from DBA_CONFIG.dbo.DBA_Server_List WHERE [BLOCKINGCHECK]=1"

$instances = Invoke-Sqlcmd -Query $query_serverlist -ServerInstance $serverName -Database $databasename -querytimeout 0

foreach ( $instance in $instances ) 
{
    #Write-Host "Instance: $($instance.name)" 
    ## Run the blocking cmdlets
    Get-SQLBlocking -Server "$($instance.name)" -DurationToReport 60000
    
}