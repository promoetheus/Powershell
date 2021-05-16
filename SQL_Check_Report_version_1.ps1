##SQL Server Health Check Report
##Created by Atul Kapoor
##Date Create: Nov 25th, 2014
##Version : 1

#Add-PSSnapin SqlServerCmdletSnapin110
#Add-PSSnapin SqlServerProviderSnapin110

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

cls

$starttime = Get-Date
write-host $starttime
#$servernames=Read-Host "Please enter a SQL Server Name"
$servernames = get-content E:\MSSQL\Powershell\SQLServerHealthCheckScripts\Server.txt
foreach($servername in $servernames)
{
write-host Starting Server $servername
$dataSource  =  $servername
##setup data source

$database = "master"                                 ##Database name
$TableHeader = "SQL Server Health Check Report"      ##The title of the HTML page
#$OutputFile = "C:\dba_temp\MyReport.htm"            ##The file location
$path = "E:\MSSQL\Powershell\SQLServerHealthCheckScripts\"
$name = $dataSource -replace "\\","_"
$OutputFile_new = $path + $name + '.html'             ##The file location

$a = "<style>"
$a = $a + "BODY{background-color:peachpuff;}"
$a = $a + "TABLE{border-width: 1px;border-style: solid;border-color: black;border-collapse: collapse;}"
$a = $a + "TH{border-width: 1px;padding: 1px;border-style: solid;border-color: black;;background-color:thistle}"
$a = $a + "TD{border-width: 1px;padding: 0px;border-style: solid;border-color: black;}"
$a = $a + "</style>"

$colorTagTable = @{
                    Stopped = ' bgcolor="RED">Stopped<';
                    Running = ' bgcolor="Green">Running<';
                    OFFLINE = ' bgcolor="RED">OFFLINE<';
                    ONLINE  = ' bgcolor="Green">ONLINE<'
                    "ALL DATABASES ARE" = ' bgcolor="Green">ALL DATABASES ARE<'
                    "ALL Databases has been" = ' bgcolor="Green">ALL Databases has been<';
                    "backup" = ' bgcolor="Green">backup<';
                    "in Last 24 Hours" = ' bgcolor="Green">in Last 24 Hours<';
                    "No Job Failed in Last 24 Hours" = ' bgcolor="Green">No Job Failed in Last 24 Hours<';
                    "Error Log" = ' bgcolor="Green">Error Log<';
                    "check did not find out anything major" = ' bgcolor="Green">check didnot find out anything major<';
                    "but will still advise to please verify manually" = ' bgcolor="Green">but will still advise to please verify manually<';
                    "Server Might Have Memory Issue"  = ' bgcolor="Red">Server Might Have Memory Issue<';
                   }



##Create a string variable with all our connection details 
$connectionDetails = "Provider=sqloledb; " + "Data Source=$dataSource; " + "Initial Catalog=$database; " + "Integrated Security=SSPI;"


##**************************************
##Calculating SQL Server Information
##**************************************
$sql_server_info = "select @@servername as [SQLNetworkName], 
CAST( SERVERPROPERTY('MachineName') AS NVARCHAR(128)) AS [MachineName],
CAST( SERVERPROPERTY('ServerName')AS NVARCHAR(128)) AS [SQLServerName],
CAST( SERVERPROPERTY('IsClustered') AS NVARCHAR(128)) AS [IsClustered],Node 
CAST( SERVERPROPERTY('ComputerNamePhysicalNetBIOS')AS NVARCHAR(128)) AS [SQLService_Current_Node],
serverproperty('edition') as [Edition],
serverproperty('productlevel') as [Servicepack],
CAST( SERVERPROPERTY('InstanceName') AS NVARCHAR(128)) AS [InstanceName],
SERVERPROPERTY('Productversion') AS [ProductVersion],@@version as [Serverversion]"

##Connect to the data source using the connection details and T-SQL command we provided above, and open the connection
$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command1 = New-Object System.Data.OleDb.OleDbCommand $sql_server_info,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command1
$dataSet1 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet1)
$connection.Close()


##Return all of the rows and pipe it into the ConvertTo-HTML cmdlet, and then pipe that into our output file
$frag1 = $dataSet1.Tables | Select-Object -Expand Rows |select -Property SQLNetworkName,MachineName,SQLServerName,IsClustered,SQLService_Current_Node,Edition,Servicepack,InstanceName,ProductVersion,Serverversion  | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQL Server Info</h2>'|Out-String


write-host $frag1

##**************************************
##SQL Server AGent Information Collection
##**************************************
$sqlserverAgent = "
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE ID = OBJECT_ID(N'tempdb..#sql_agent_state'))
BEGIN
drop table #sql_agent_state
    END
    declare @sql_agent_service varchar(128),@state_sql_agent varchar(20)
    create table #sql_agent_state(service_name varchar(128) default 'SQLAgent ' ,state varchar(20))
    insert into #sql_agent_state(state) exec xp_servicecontrol N'querystate',N'SQLServerAGENT'  
    --select service_name as ServiceName, state as Status from #sql_agent_state
    select service_name as ServiceName, replace(state,'.','') as Status from #sql_agent_state
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command2 = New-Object System.Data.OleDb.OleDbCommand $sqlserverAgent,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command2
$dataSet2 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet2)
$connection.Close()

$frag2 = $dataSet2.Tables | Select-Object -Expand Rows| Select -Property ServiceName,Status|ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQL Server Agent Status</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag2 = $frag2 -replace ">$_<",($colorTagTable.$_) }

write-host $frag2

##**************************************
##Database states
##**************************************
$SQLServerDatabaseState = "
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE ID = OBJECT_ID(N'tempdb..#tmp_database'))
BEGIN
drop table #tmp_database
END

declare @count int
declare @name varchar(128)
declare @state_desc varchar(128)

select @count = COUNT(*) from sys.databases where state_desc not in ('ONLINE','RESTORING')
create table #tmp_database (name nvarchar(128),state_desc nvarchar(128))
if @count > 0
		begin
			Declare Cur1 cursor for select name,state_desc from sys.databases 
			where state_desc not in ('ONLINE','RESTORING')
		open Cur1
			FETCH NEXT FROM Cur1 INTO @name,@state_desc
			WHILE @@FETCH_STATUS = 0
				BEGIN
					insert into #tmp_database values(@name,@state_desc)
				FETCH NEXT FROM Cur1 INTO @name,@state_desc
				END
			CLOSE Cur1
			DEALLOCATE Cur1
		end
else 
	begin
		insert into #tmp_database values('ALL DATABASES ARE','ONLINE')
	end

select name as DBName ,state_desc as DBStatus from #tmp_database
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command3 = New-Object System.Data.OleDb.OleDbCommand $SQLServerDatabaseState,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command3
$dataSet3 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet3)
$connection.Close()

$frag3 = $dataSet3.Tables | Select-Object -Expand Rows |Select -Property DBName,DBStatus | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQLServer Databases State</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag3 = $frag3 -replace ">$_<",($colorTagTable.$_) }

write-host $frag3




##**************************************
##SQL Job Status
##**************************************
$SQLJob = "
declare @count int
select @count = count(1) from msdb.dbo.sysjobs as sj 
join msdb.dbo.sysjobhistory as sjh on sj.job_id = sjh.job_id 
where sj.enabled != 0 
and sjh.sql_message_id > 0 
and sjh.run_date > CONVERT(char(8), (select dateadd (day,(-1), getdate())), 112)
and sjh.Step_id <= 1

if (@count >= 1)
begin
	select distinct sj.name as SQLJobName
	from msdb.dbo.sysjobs as sj 
	join msdb.dbo.sysjobhistory as sjh on sj.job_id = sjh.job_id 
	where sj.enabled != 0 
	and sjh.sql_message_id > 0 
	and sjh.run_date > CONVERT(char(8), (select dateadd (day,(-1), getdate())), 112)
	and sjh.Step_id <= 1
	order by name
end
else 
begin
	Select 'No Job Failed in Last 24 Hours' as SQLJobName
end

"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command4 = New-Object System.Data.OleDb.OleDbCommand $SQLJob,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command4
$dataSet4 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet4)
$connection.Close()

$frag4 = $dataSet4.Tables | Select-Object -Expand Rows |select -Property SQLJobName | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQLServer SQL Job failed in last 24 Hours</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag4 = $frag4 -replace ">$_<",($colorTagTable.$_) }

write-host $frag4


##**************************************
##Database Backup in Last 24 Hours
##**************************************
$SQLServerDatabaseBackup = "
declare @backupcount int
select @backupcount = count(1)
 from sys.databases 
where state != 6 
and name not like 'Tempdb%' 
and name not in 
(
select database_name
from msdb.dbo.backupset as bkupset 
join msdb.dbo.backupmediafamily as bkupmedf on bkupset.media_set_id = bkupmedf.media_set_id 
where type in ('D','I')
and backup_start_date > (CONVERT(datetime,getdate()) - 1)
)

if (@backupcount >= 1)
begin
select name as DBName, State_Desc as DBStatus,'Backup Not happened' as DBComments from sys.databases 
where state != 6 
and name not like 'Tempdb%' 
and name not in 
(
select database_name
from msdb.dbo.backupset as bkupset 
join msdb.dbo.backupmediafamily as bkupmedf on bkupset.media_set_id = bkupmedf.media_set_id 
where type in ('D','I')
and backup_start_date > (CONVERT(datetime,getdate()) - 1)
)
order by 1
end
else 
begin
Select 'ALL Databases has been' as DBName, 'backup' as DBStatus ,'in Last 24 Hours' as DBComments
end
"
$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command5 = New-Object System.Data.OleDb.OleDbCommand $SQLServerDatabaseBackup,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command5
$dataSet5 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet5)
$connection.Close()

$frag5 = $dataSet5.Tables | Select-Object -Expand Rows |select -property DBName,DBStatus,DBComments | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQLServer Database Backup status in Last 24 Hours</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag5 = $frag5 -replace ">$_<",($colorTagTable.$_) }

write-host $frag5


##**************************************
##SQL Server ErrorLog
##**************************************
$SQLServerErrorlog = "
declare @errorlogcount int
IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects WHERE ID = OBJECT_ID(N'tempdb..#errorlog'))
BEGIN
DROP TABLE #errorlog
END
create table #errorlog(date_time datetime,processinfo varchar(123),Comments varchar(max))
insert into #errorlog exec sp_readerrorlog

select @errorlogcount = count(*) from #errorlog 
where date_time > (CONVERT(datetime,getdate()) - 0.5)
and Comments like '%fail%' 
and Comments like '%error%'
and processinfo not in ('Server','Logon')

if(@errorlogcount >= 1)
begin
select date_time as Date,processinfo as ProcessInfo, Comments from #errorlog 
where date_time > (CONVERT(datetime,getdate()) - 0.5)
and Comments like '%fail%' 
and Comments like '%error%'
and processinfo not in ('Server','Logon')
end
else
begin
select 'Error Log' as Date, 'check did not find out anything major' as ProcessInfo, 'but will still advise to please verify manually' as Comments
end
"


$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command6 = New-Object System.Data.OleDb.OleDbCommand $SQLServerErrorlog,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command6
$dataSet6 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet6)
$connection.Close()

$frag6 = $dataSet6.Tables | Select-Object -Expand Rows|select -Property Date,processinfo,Comments | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>SQLServer ErroLog Information</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag6 = $frag6 -replace ">$_<",($colorTagTable.$_) }

write-host $frag6

##**************************************
##CPU information
##**************************************
$SQLServerCPUInformation = "declare @query2008r2_cpu nvarchar(max)
declare @query2012_cpu nvarchar(max)
set @query2008r2_cpu = 'SELECT cpu_count AS Logical_CPU_Count, hyperthread_ratio AS Hyperthread_Ratio,
			cpu_count/hyperthread_ratio AS Physical_CPU_Count,physical_memory_in_bytes/1024/1024 AS Physical_Memory_in_MB
			FROM sys.dm_os_sys_info'

set @query2012_cpu = 'SELECT cpu_count AS Logical_CPU_Count, hyperthread_ratio AS Hyperthread_Ratio,
			cpu_count/hyperthread_ratio AS Physical_CPU_Count,physical_memory_kb/1024/1024 AS Physical_Memory_in_MB
			FROM sys.dm_os_sys_info'


/*SQL Object Memory Allocation*/
declare @version nvarchar(128)
select @version = cast(SERVERPROPERTY('Productversion') as nvarchar(128))
if (@version like '11%')
	begin
		EXECUTE sp_executesql @query2012_cpu	
	end
	else
	begin
		EXECUTE sp_executesql @query2008r2_cpu
	end
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command7 = New-Object System.Data.OleDb.OleDbCommand $SQLServerCPUInformation,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command7
$dataSet7 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet7)
$connection.Close()

$frag7 = $dataSet7.Tables | Select-Object -Expand Rows|select -Property Logical_CPU_Count,Hyperthread_Ratio,Physical_CPU_Count,Physical_Memory_in_MB  | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>CPU Information</h2>'|Out-String
write-host $frag7

##**************************************
##SQL Server Memory Infomration
##**************************************

## 1. Memory Allocated to SQL Server

$SQLServerMemoryAllocated = "SELECT --object_name,
counter_name as Counter, cntr_value/1024 as MemoryLimitSet_inMB 
FROM sys.dm_os_performance_counters
WHERE counter_name IN ('Total Server Memory (KB)', 'Target Server Memory (KB)');"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command8 = New-Object System.Data.OleDb.OleDbCommand $SQLServerMemoryAllocated,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command8
$dataSet8 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet8)
$connection.Close()

$frag8 = $dataSet8.Tables | Select-Object -Expand Rows|select -Property Counter,MemoryLimitSet_inMB | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Memory Allocated to SQL Server</h2>'|Out-String
write-host $frag8


#2. Top 10 Memory Consuing Objects
$SqlServerMemortConsumingobjects = "declare @query2008r2_and_less nvarchar(max)
declare @query2012_and_more nvarchar(max)

set @query2008r2_and_less = 'select top 10 type as Object,SUM(single_pages_kb+multi_pages_kb+virtual_memory_committed_kb+awe_allocated_kb)/1024 as Space_used_inMB
		from sys.dm_os_memory_clerks 
		group by type
		order by 2 desc'

set @query2012_and_more = 'select top 10 type as Object,SUM(pages_kb+virtual_memory_committed_kb+awe_allocated_kb)/1024 as Space_used_inMB
		from sys.dm_os_memory_clerks 
		group by type
		order by 2 desc'


/*SQL Object Memory Allocation*/
declare @version nvarchar(128)
select @version = cast(SERVERPROPERTY('Productversion') as nvarchar(128))

if (@version like '11%')
	begin
		EXECUTE sp_executesql @query2012_and_more	
	end
	else
	begin
		EXECUTE sp_executesql @query2008r2_and_less
	end
"
$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command9 = New-Object System.Data.OleDb.OleDbCommand $SqlServerMemortConsumingobjects,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command9
$dataSet9 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet9)
$connection.Close()

$frag9 = $dataSet9.Tables | Select-Object -Expand Rows | select -Property Object,Space_used_inMB  | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Top 10 Memory Consuming SQL Objects</h2>'|Out-String
write-host $frag9


#3.     
$sqlservermemorypressuredetection = "declare @totalmemoryused bigint
declare @bufferpool_allocated bigint
declare @query2008r2_total nvarchar(max)
declare @query2012_total nvarchar(max)
declare @version nvarchar(128)

Set @query2008r2_total  = 'select SUM(single_pages_kb+multi_pages_kb+virtual_memory_committed_kb+awe_allocated_kb)/1024
from sys.dm_os_memory_clerks' 
 
set @query2012_total = 'select SUM(pages_kb+virtual_memory_committed_kb+awe_allocated_kb)/1024
from sys.dm_os_memory_clerks'

select @version = cast(SERVERPROPERTY('Productversion') as nvarchar(128))
--select @version
if (@version like '11%')
	begin
		create table #tmp (value bigint)
		insert into #tmp Execute (@query2012_total)	
		select @totalmemoryused = value from #tmp
		drop table #tmp
	end
else
	begin
		create table #tmp_1 (value bigint)
		insert into #tmp_1 Execute (@query2008r2_total)	
		select @totalmemoryused = value from #tmp_1
		drop table #tmp_1
	end

--select @totalmemoryused

select @bufferpool_allocated = cntr_value/1024
FROM sys.dm_os_performance_counters
WHERE counter_name IN ('Target Server Memory (KB)')

if (@bufferpool_allocated > @totalmemoryused)
    begin
        Select 'Server has no Memory Issue' as Comments
    end 
else 
    begin 
        select 'Server Might Have Memory Issue' as Comments
    end
"
$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command10 = New-Object System.Data.OleDb.OleDbCommand $sqlservermemorypressuredetection,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command10
$dataSet10 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet10)
$connection.Close()

$frag10 = $dataSet10.Tables | Select-Object -Expand Rows |select -Property Comments | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Server Have Memory Pressure or Not</h2>'|Out-String

$colorTagTable.Keys | foreach { $frag10 = $frag10 -replace ">$_<",($colorTagTable.$_) }

write-host $frag10


##**************************************
##Top 10 Long Running Queries
##**************************************
$LongRunningQueries = "
SELECT TOP 10 DB_NAME(qt.dbid) AS DBName,
o.name AS ObjectName,
qs.total_worker_time / 1000000 / qs.execution_count As Avg_MultiCore_CPU_time_sec,
qs.total_worker_time / 1000000 as Total_MultiCore_CPU_time_sec,
qs.total_elapsed_time / qs.execution_count / 1000000.0 AS Average_Seconds,
qs.total_elapsed_time / 1000000.0 AS Total_Seconds,
qs.execution_count as Count,
qs.last_execution_time as Time,
SUBSTRING (qt.text,qs.statement_start_offset/2,
(CASE WHEN qs.statement_end_offset = -1
THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) AS Query
--,qt.text
--,qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
LEFT OUTER JOIN sys.objects o ON qt.objectid = o.object_id
where last_execution_time > getdate()-1
ORDER BY average_seconds DESC
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command11 = New-Object System.Data.OleDb.OleDbCommand $LongRunningQueries,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command11
$dataSet11 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet11)
$connection.Close()

$frag11 = $dataSet11.Tables | Select-Object -Expand Rows|select -Property DBName,ObjectName,Avg_MultiCore_CPU_time_sec,
Total_MultiCore_CPU_time_sec,Average_Seconds,Total_Seconds,Count,Time,Query| ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Top 10 Long Running Query</h2>'|Out-String
write-host $frag11


##**************************************
##Top 10 CPU Consuming Query
##**************************************
$CPUConsumingQuery = "
SELECT TOP 10 DB_NAME(qt.dbid) as DBName,
	o.name AS ObjectName,
	qs.total_worker_time / 1000000 / qs.execution_count AS Avg_MultiCore_CPU_time_sec,
	qs.total_worker_time / 1000000 As Total_MultiCore_CPU_time_sec,
	qs.total_elapsed_time / 1000000 / qs.execution_count As Average_Seconds,
	qs.total_elapsed_time / 1000000 As Total_Seconds,
    (total_logical_reads + total_logical_writes) / qs.execution_count as Average_IO,
	total_logical_reads + total_logical_writes as Total_IO,	
    qs.execution_count as Count,
    qs.last_execution_time as Time,
	SUBSTRING(qt.[text], (qs.statement_start_offset / 2) + 1,
		(
			(
				CASE qs.statement_end_offset
					WHEN -1 THEN DATALENGTH(qt.[text])
					ELSE qs.statement_end_offset
				END - qs.statement_start_offset
			) / 2
		) + 1
	) as Query
    --,qt.text
	--,qp.query_plan
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.[sql_handle]) AS qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
LEFT OUTER JOIN sys.objects o ON qt.objectid = o.object_id
where qs.execution_count > 5	--more than 5 occurences
ORDER BY Total_MultiCore_CPU_time_sec DESC
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command12 = New-Object System.Data.OleDb.OleDbCommand $CPUConsumingQuery,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command12
$dataSet12 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet12)
$connection.Close()

$frag12 = $dataSet12.Tables | Select-Object -Expand Rows |select -Property DBName,ObjectName,Avg_MultiCore_CPU_time_sec,
Total_MultiCore_CPU_time_sec,Average_Seconds,Total_Seconds,Average_IO,Total_IO,Count,Time,Query | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Top 10 CPU Consuming Query</h2>'|Out-String
write-host $frag12


##**************************************
##Top 10 IO Consuming Query
##**************************************
$IOConsumingQuery = "
SELECT TOP 10 DB_NAME(qt.dbid) AS DBName,
o.name AS ObjectName,
qs.total_elapsed_time / 1000000 / qs.execution_count As Average_Seconds,
qs.total_elapsed_time / 1000000 As Total_Seconds,
(total_logical_reads + total_logical_writes ) / qs.execution_count AS Average_IO,
(total_logical_reads + total_logical_writes ) AS Total_IO,
qs.execution_count AS Count,
last_execution_time As Time,
SUBSTRING (qt.text,qs.statement_start_offset/2,
(CASE WHEN qs.statement_end_offset = -1
THEN LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2
ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) AS Query
--,qt.text
--,qp.query_plan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) as qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
LEFT OUTER JOIN sys.objects o ON qt.objectid = o.object_id
where last_execution_time > getdate()-1
ORDER BY average_IO DESC
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command13 = New-Object System.Data.OleDb.OleDbCommand $IOConsumingQuery,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command13
$dataSet13 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet13)
$connection.Close()

$frag13 = $dataSet13.Tables | Select-Object -Expand Rows |Select -Property DBName,ObjectName,Average_Seconds,
Total_Seconds, Average_IO, Total_IO, Count, Time, Query | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Top 10 IO Consuming Query</h2>'|Out-String
write-host $frag13


##**************************************
##CPU Pressure
##**************************************
$CPUPressure = "
SELECT CAST(100.0 * SUM(signal_wait_time_ms) / SUM (wait_time_ms) AS NUMERIC(20,2)) AS Pct_Signal_CPU_Waits,
'if Perc_signal_cpu_waits is > 15%, it means we have CPU pressure' as Comment,
CAST(100.0 * SUM(wait_time_ms - signal_wait_time_ms) / SUM (wait_time_ms) AS NUMERIC(20,2)) AS Pct_Resource_Waits
FROM sys.dm_os_wait_stats
"

$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command14 = New-Object System.Data.OleDb.OleDbCommand $CPUPressure,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command14
$dataSet14 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet14)
$connection.Close()

$frag14 = $dataSet14.Tables | Select-Object -Expand Rows | select -Property Pct_Signal_CPU_Waits,Comment,
Pct_Resource_Waits | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>CPU Pressure Detection</h2>'|Out-String
write-host $frag14

##**************************************
##Wait Type % Calc
##**************************************

$WaitTimePercentage = "declare @totalwait_time_ms float
select @totalwait_time_ms  = sum(wait_time_ms) 
FROM sys.dm_os_wait_stats
where wait_time_ms > 0

select top 10 wait_type as WaitEvent,
	wait_time_ms/1000 as Time_inSec,
	round(100*(cast(wait_time_ms as float)/@totalwait_time_ms),2) as PctUsed
FROM sys.dm_os_wait_stats 
where wait_time_ms > 0
order by wait_time_ms desc
"
$connection = New-Object System.Data.OleDb.OleDbConnection $connectionDetails
$command15 = New-Object System.Data.OleDb.OleDbCommand $WaitTimePercentage,$connection
$connection.Open()

##Get the results of our command into a DataSet object, and close the connection
$dataAdapter = New-Object System.Data.OleDb.OleDbDataAdapter $command15
$dataSet15 = New-Object System.Data.DataSet
$dataAdapter.Fill($dataSet15)
$connection.Close()

$frag15 = $dataSet15.Tables | Select-Object -Expand Rows |select -Property WaitEvent,Time_inSec,PctUsed | ConvertTo-HTML -AS Table -Fragment -PreContent '<h2>Wait Type % Allocation on Server</h2>'|Out-String
write-host $frag15


##**************************************
##Final Code to Combine all fragments
##**************************************

ConvertTo-HTML -head $a -PostContent $frag1,$frag2,$frag3,$frag4,$frag5,$frag6,$frag7,$frag8,$frag9,$frag10,$frag14,$frag15,$frag11,$frag12,$frag13 -PreContent "<h1>SQL Server Heatlh Check Report</h1>" | Out-File $OutputFile_new

$smtpServer = "mail.worldpay.local"
$anonUsername = "anonymous"
$anonPassword = ConvertTo-SecureString -String "anonymous" -AsPlainText -Force
$anonCredentials = New-Object System.Management.Automation.PSCredential($anonUsername,$anonPassword)

$attachment = "$OutputFile_new"
$body= Get-Content $OutputFile_new
#write-host $body

$subject = "Health Check Report for Server: " + $servername

write-host "Sending email"

Send-MailMessage -to "cibi.john@worldpay.com" -from "noreply@SQLServerDBMail.worldpay.com" -SmtpServer "mail.worldpay.local" -subject $subject -credential $anonCredentials -BodyAsHtml "$body" -Attachments $attachment #$OutputFile_new


$Stoptime = Get-Date
Write-host $Stoptime

}
remove-variable starttime
remove-variable servernames
remove-variable servername
remove-variable dataSource
remove-variable database
remove-variable TableHeader
remove-variable path
remove-variable name
remove-variable OutputFile_new
remove-variable a
remove-variable colorTagTable
remove-variable connectionDetails
remove-variable connection
remove-variable dataAdapter
remove-variable sql_server_info
remove-variable sqlserverAgent
remove-variable SQLServerDatabaseState
remove-variable SQLJob
remove-variable SQLServerDatabaseBackup
remove-variable SQLServerErrorlog
remove-variable SQLServerCPUInformation
remove-variable SQLServerMemoryAllocated
remove-variable SqlServerMemortConsumingobjects
remove-variable sqlservermemorypressuredetection
remove-variable LongRunningQueries
remove-variable CPUConsumingQuery
remove-variable IOConsumingQuery
remove-variable CPUPressure
remove-variable WaitTimePercentage
remove-variable smtpServer
remove-variable anonUsername
remove-variable anonPassword
remove-variable anonCredentials
remove-variable attachment
remove-variable body
remove-variable subject
remove-variable Stoptime

remove-variable dataSet1
remove-variable dataSet2
remove-variable dataSet3
remove-variable dataSet4
remove-variable dataSet5
remove-variable dataSet6
remove-variable dataSet7
remove-variable dataSet8
remove-variable dataSet9
remove-variable dataSet10
remove-variable dataSet11
remove-variable dataSet12
remove-variable dataSet13
remove-variable dataSet14
remove-variable dataSet15

remove-variable command1
remove-variable command2
remove-variable command3
remove-variable command4
remove-variable command5
remove-variable command6
remove-variable command7
remove-variable command8
remove-variable command9
remove-variable command10
remove-variable command11
remove-variable command12
remove-variable command13
remove-variable command14
remove-variable command15

remove-variable frag1
remove-variable frag2
remove-variable frag3
remove-variable frag4
remove-variable frag5
remove-variable frag6
remove-variable frag7
remove-variable frag8
remove-variable frag9
remove-variable frag10
remove-variable frag11
remove-variable frag12
remove-variable frag13
remove-variable frag14
remove-variable frag15
