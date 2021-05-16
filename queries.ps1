if ($versionNo > 10)
{
$query_bck_database = "select [Database]=d.name,LastFull,LastTran,GetDate=getdate(), RecoveryMode=DATABASEPROPERTYEX(d.name, 'Recovery'),
CreationTime=d.crdate, Status=DATABASEPROPERTYEX(d.name, 'Status')

 from master.dbo.sysdatabases d
left outer join
 (select database_name, LastFull=max(backup_finish_date)
        from msdb.dbo.backupset
        where type = 'D' and backup_finish_date <= getdate()
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
 where d.name <> 'Tempdb' AND sys.fn_hadr_backup_is_preferred_replica( d.name) = 1
order by [LastFull]";
}
else
{
$query_bck_database = "select [Database]=d.name,LastFull,LastTran,GetDate=getdate(), RecoveryMode=DATABASEPROPERTYEX(d.name, 'Recovery'),
CreationTime=d.crdate, Status=DATABASEPROPERTYEX(d.name, 'Status')

 from master.dbo.sysdatabases d
left outer join
 (select database_name, LastFull=max(backup_finish_date)
        from msdb.dbo.backupset
        where type = 'D' and backup_finish_date <= getdate()
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
 where d.name <> 'Tempdb'
order by [LastFull]";
}



# isinstandby=0
# status=online 
# (not: offline, restoring, recovering, suspect, emergency)
# databaseproperty != mirror, recover, offline

<#
$query_bck_database = "select [Database]=d.name,LastFull,LastTran,GetDate=getdate(), RecoveryMode=DATABASEPROPERTYEX(d.name, 'Recovery'),
CreationTime=d.crdate, Status=DATABASEPROPERTYEX(d.name, 'Status')

 from master.dbo.sysdatabases d
left outer join
 (select database_name, LastFull=max(backup_finish_date)
        from msdb.dbo.backupset
        where type = 'D' and backup_finish_date <= getdate()
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
 where d.name <> 'Tempdb'
order by [LastFull]";

#>


# isinstandby=0
# status=online 
# (not: offline, restoring, recovering, suspect, emergency)
# databaseproperty != mirror, recover, offline