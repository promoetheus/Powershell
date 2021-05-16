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

$instances = invoke-sqlcmd -Query "Select ID, [name]=[server] from dbo.DBA_Server_List WHERE JOBHISTORYCHECK=1" -ServerInstance $serverName -Database $databasename

foreach ( $instance in $instances ) 
{

    $query_server_jobhistory = "IF OBJECT_ID('tempdb..#JobResults') IS NOT NULL DROP TABLE #JobResults
        GO

CREATE TABLE #JobResults
    (job_id uniqueidentifier NOT NULL, 
    last_run_date int NOT NULL, 
    last_run_time int NOT NULL, 
    next_run_date int NOT NULL, 
    next_run_time int NOT NULL, 
    next_run_schedule_id int NOT NULL, 
    requested_to_run int NOT NULL, /* bool*/ 
    request_source int NOT NULL, 
    request_source_id sysname 
    COLLATE database_default NULL, 
    running int NOT NULL, /* bool*/ 
    current_step int NOT NULL, 
    current_retry_attempt int NOT NULL, 
    job_state int NOT NULL) 

INSERT    #JobResults 
EXEC master.dbo.xp_sqlagent_enum_jobs 1, '';

;WITH LatestJobHistory_CTE AS
(
	SELECT 
		instance_Id,
		job_id,
		run_date,
		run_time,
		run_status,
		ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY instance_id DESC) AS RONO
	from msdb.dbo.sysjobhistory
	WHERE step_id = 0
)

SELECT    
    r.job_id, 
    job.name as Job_Name, 
    (select top 1 start_execution_date 
            FROM [msdb].[dbo].[sysjobactivity]
            where job_id = r.job_id
            order by start_execution_date desc) as Job_Start_DateTime,
            
    cast((select top 1 ISNULL(stop_execution_date, GETDATE()) - start_execution_date  
            FROM [msdb].[dbo].[sysjobactivity]
            where job_id = r.job_id
            order by start_execution_date desc) as time) as Job_Duration, 
	CASE WHEN r.running = 1 THEN 'Job In Progress'
		ELSE jobInfo.last_outcome_message 
	END As [Message],
    CASE 
        WHEN r.running = 0 then jobinfo.last_run_outcome
        ELSE
            --convert to the uniform status numbers (my design)
            CASE
                WHEN r.job_state = 0 THEN 1    --success
                WHEN r.job_state = 4 THEN 1
                WHEN r.job_state = 5 THEN 1
                WHEN r.job_state = 1 THEN 2    --in progress
                WHEN r.job_state = 2 THEN 2
                WHEN r.job_state = 3 THEN 2
                WHEN r.job_state = 7 THEN 2
            END
    END as Run_Status,
    CASE 
        WHEN r.running = 0 then 
            CASE 
                WHEN jobInfo.last_run_outcome = 0 THEN 'Failed'
                WHEN jobInfo.last_run_outcome = 1 THEN 'Success'
                WHEN jobInfo.last_run_outcome = 3 THEN 'Canceled'
                ELSE 'Unknown'
            end
            WHEN r.job_state = 0 THEN 'Success'
            WHEN r.job_state = 4 THEN 'Success'
            WHEN r.job_state = 5 THEN 'Success'
            WHEN r.job_state = 1 THEN 'In Progress'
            WHEN r.job_state = 2 THEN 'In Progress'
            WHEN r.job_state = 3 THEN 'In Progress'
            WHEN r.job_state = 7 THEN 'In Progress'
         ELSE 'Unknown' END AS Run_Status_Description,
		 GETDATE() As CollectionDate,
		 r.current_step AS Current_Running_Step_ID,
		 jobInfo.last_run_outcome As LastRunOutcome,
		 LJH.Instance_ID 
FROM    #JobResults as r LEFT JOIN
        msdb.dbo.sysjobservers as jobInfo on r.job_id = jobInfo.job_id inner join
        msdb.dbo.sysjobs as job on r.job_id = job.job_id INNER JOIN 
		LatestJobHistory_CTE LJH ON r.job_id = LJH.job_id AND LJH.RONO = 1
--WHERE  job.[enabled] = 1 
ORDER BY job.name;

DROP TABLE #JobResults"

    #Write-Output "    $query_server_jobhistory"
    $serverdbs  = invoke-sqlcmd -Query $query_server_jobhistory -ServerInstance $instance.name 
    
    foreach ($serverdb in $serverdbs)
    { 
           $Command.CommandText = "MERGE dbo.DBA_JobHistory as target USING (
            Select $($instance.ID),'$($instance.Name )','$($serverdb.job_id)','$($serverdb.job_Name)','$($serverdb.job_Start_DateTime)','$($serverdb.job_Duration)'
                ,'$($serverdb.Message)',$($serverdb.Run_status),'$($serverdb.Run_Status_Description)','$($serverdb.CollectionDate)',$($serverdb.Current_Running_Step_ID)
                ,$($serverdb.LastRunOutcome),$($serverdb.Instance_ID)
            )
            AS source (InstanceID,InstanceName, JobID, JobName,JobStartDateTime, JobDuration, Message,RunStatus, RunStatusDesc,CollectionDate,CurrentStep,LastRunOutcome, HistoryID)
            ON (source.InstanceID  = target.ServerInstanceID and source.JobID=target.Job_ID)
            WHEN MATCHED THEN
                UPDATE SET JobStartDate=source.JobStartDateTime,JobRunDuration=CASE WHEN Target.Run_Status=0 AND Target.JobStartDate=source.JobStartDateTime THEN Target.JobRunDuration ELSE source.JobDuration END,Message=source.Message,Run_Status=source.RunStatus,
                    Run_StatusDesc=source.RunStatusDesc,CollectionDate=source.CollectionDate,CurrentRunningStepID=source.CurrentStep,
                    LastRunOutCome=source.LastRunOutcome,JobInstanceID=source.HistoryID
            WHEN NOT MATCHED THEN
                INSERT (ServerInstanceID,ServerInstanceName,Job_ID,Job_Name,JobStartDate,JobRunDuration,Message,Run_Status,Run_StatusDesc,CollectionDate,CurrentRunningStepID,LastRunOutCome,JobInstanceID)
                VALUES (source.InstanceID, source.InstanceName, source.JobID, source.JobName, source.JobStartDateTime, source.JobDuration, source.Message,source.RunStatus, 
                source.RunStatusDesc,source.CollectionDate,source.CurrentStep,source.LastRunOutcome, source.HistoryID);"
            #Write-Output " $($Command.CommandText)"
            $Command.ExecuteNonQuery() | out-null
        


            $query_server_jobhistorydetail = "DECLARE @Job_ID as uniqueidentifier; SET @Job_ID = '$($serverdb.job_id)';
                        DECLARE @Job_Start_DateTime as smalldatetime
                        SET @Job_Start_DateTime = (select top 1 start_execution_date 
                            FROM [msdb].[dbo].[sysjobactivity]
                            where job_id = @Job_ID
                            order by start_execution_date desc)
                        SELECT         
                            Steps.step_id, 
                            Steps.step_name, 
                            run_status, 
                            run_status_description, 
                            Step_Start_DateTime,
                            Step_Duration,
	                        Step_Message,
	                        instance_id
                        FROM            
                            (SELECT        
                                Jobstep.step_name, 
                                Jobstep.step_id
                            FROM    msdb.dbo.sysjobsteps AS Jobstep
                            WHERE job_id = @Job_ID) AS Steps LEFT JOIN
    
                            (SELECT
                                 JobHistory.step_id, 
                                 CASE --convert to the uniform status numbers we are using
                                    WHEN JobHistory.run_status = 0 THEN 0
                                    WHEN JobHistory.run_status = 1 THEN 1
                                    WHEN JobHistory.run_status = 2 THEN 2
                                    WHEN JobHistory.run_status = 4 THEN 2
                                    WHEN JobHistory.run_status = 3 THEN 3
                                    ELSE 'Unknown' 
                                 END AS run_status, 
                                 CASE 
                                    WHEN JobHistory.run_status = 0 THEN 'Failed' 
                                    WHEN JobHistory.run_status = 1 THEN 'Success' 
                                    WHEN JobHistory.run_status = 2 THEN 'In Progress'
                                    WHEN JobHistory.run_status = 4 THEN 'In Progress' 
                                    WHEN JobHistory.run_status = 3 THEN 'Canceled' 
                                    ELSE 'Unknown' 
                                 END AS run_status_description,
                                 convert(datetime,convert(varchar(100),run_date) + ' ' 
		                        + convert(varchar(2),run_time/10000)
		                        + ':' + convert(varchar(2),(run_time/100) % 100) 
                                + ':' + convert(varchar(2),run_time % 100))  as Step_Start_DateTime,
                                 CAST(CAST(STUFF(STUFF(REPLACE(STR(JobHistory.run_duration % 240000, 6, 0), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME) AS TIME) AS Step_Duration,
		                         message As Step_Message,
		                         instance_id
                            FROM msdb..sysjobhistory as JobHistory WITH (NOLOCK) 
                            WHERE job_id = @Job_ID and convert(datetime,convert(varchar(100),run_date) + ' ' 
		                        + convert(varchar(2),run_time/10000)
		                        + ':' + convert(varchar(2),(run_time/100) % 100) 
                                + ':' + convert(varchar(2),run_time % 100)) >= @Job_Start_DateTime 
                            ) AS StepStatus ON Steps.step_id = StepStatus.step_id 
                        ORDER BY Steps.step_id"
                #Write-Output "    $query_server_jobhistorydetail"    

                    
                $historydb  = invoke-sqlcmd -Query $query_server_jobhistorydetail -ServerInstance $instance.name 
            
                foreach ($history in $historydb)
                {
                    #Write-Output "Instance :   $($instance.name)" 
                    if ([string]::IsNullOrEmpty($($history.run_status))) { $history.run_status = $($serverdb.Run_status) }
                    if ([string]::IsNullOrEmpty($($history.Instance_ID))) { $history.Instance_ID = 0 }
                    if ([string]::IsNullOrEmpty($($history.run_status_description))) { $history.run_status_description = $($serverdb.Run_Status_Description) }


                    $history.Step_Message = $history.Step_Message -replace "'","''"
                    $Command.CommandText = "MERGE dbo.DBA_JobHistoryDetail as target USING (
                        Select $($instance.ID),'$($serverdb.job_id)',$($history.step_id),'$($history.step_name)',$($history.run_status)
                            ,'$($history.run_status_description)','$($history.Step_Start_DateTime)','$($history.Step_Duration)','$($history.Step_Message)',
                            $($history.Instance_ID), '$($serverdb.CollectionDate)'
                        )
                        AS source (InstanceID,JobID, JobStepId,JobStepName, JobRunStatus, JobRunStatusDesc,JobStepStartTime,JobRunDuration,JobStepMessage,HistoryID, CollectionDate)
                        ON (source.InstanceID  = target.ServerInstanceID and source.JobID=target.Job_ID AND source.JobStepID=target.JobStep_ID )
                        WHEN MATCHED THEN
                            UPDATE SET JobStep_Name = source.JobStepName, 
                            JobStep_Status = source.JobRunStatus, JobStep_Run_Status_Desc = source.JobRunStatusDesc,
                            JobStep_StartDatetime = source.JobStepStartTime, JobStep_RunDuration = source.JobRunDuration, 
                            JobStep_Message = source.JobStepMessage, 
                            CollectionDate = source.CollectionDate, JobStepInstanceID = source.HistoryID
                        WHEN NOT MATCHED THEN
                            INSERT (ServerInstanceID,Job_ID,JobStep_ID,JobStep_Name,JobStep_Status,JobStep_Run_Status_Desc,JobStep_StartDatetime,JobStep_RunDuration,JobStep_Message,CollectionDate,JobStepInstanceID)
                            VALUES (source.InstanceID, source.JobID, source.JobStepId,source.JobStepName, source.JobRunStatus, source.JobRunStatusDesc,
                            source.JobStepStartTime,source.JobRunDuration,source.JobStepMessage,
                            source.CollectionDate,source.HistoryID);"    
                    #Write-Output "$($Command.CommandText)"           
                    $Command.ExecuteNonQuery() | out-null

                }

    }
}

$Connection.Close()
