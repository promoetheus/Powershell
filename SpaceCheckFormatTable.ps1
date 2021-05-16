param([string] $server)

$TotalGB = @{Name="Capacity(GB)";expression={[math]::round(($_.Capacity/ 1GB),2)}}
$FreeGB = @{Name="FreeSpace(GB)";expression={[math]::round(($_.FreeSpace / 1GB),2)}}
$FreePerc = @{Name="FreeSpace (PCT)";expression={[math]::round(($_.FreeSpace/$_.Capacity*100),2)}}

function get-mountpoints {
$volumes = Get-WmiObject -computer $server win32_volume 
$volumes | Select Name, Label, $TotalGB, $FreeGB, $FreePerc | Format-Table > E:\MSSQL\Logs\DiskSpace.txt
}

get-mountpoints

#$servers = (Get-Content .\servers.txt)

#foreach ($server in $servers){
#get-mountpoints
#}

