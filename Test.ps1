E:\MSSQL\Powershell\Temp\Get-MountPointData2.ps1

$server = 'ukdc1-pc-sql12a.mgt.worldpay.local'

#Get-MountPointData -ComputerName $server | Format-Table -AutoSize

Get-MountPointData -Comp $server -IncludeRootDrives | ft -a


