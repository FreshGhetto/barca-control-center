param(
    [string]$BackupRoot = "C:\BARCA\backups",
    [string]$DockerContainer = "barca-postgres",
    [string]$DbName = "barca",
    [string]$DbUser = "barca_user",
    [string]$DbPassword = $env:BARCA_DB_PASSWORD,
    [int]$RetentionDays = 30
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $Message"
}

$dayFolder = Join-Path $BackupRoot (Get-Date -Format "yyyy-MM-dd")
New-Item -ItemType Directory -Path $dayFolder -Force | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$dumpName = "barca_${DbName}_${stamp}.dump"
$dumpPath = Join-Path $dayFolder $dumpName
$shaPath = "$dumpPath.sha256"
$tmpDump = "/tmp/$dumpName"

Write-Log "Backup start: db=$DbName container=$DockerContainer"

$inspect = & docker inspect $DockerContainer 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Container Docker non trovato: $DockerContainer"
}

$dumpCmd = @("exec")
if ($DbPassword) {
    $dumpCmd += @("-e", "PGPASSWORD=$DbPassword")
}
$dumpCmd += @($DockerContainer, "pg_dump", "-U", $DbUser, "-d", $DbName, "-Fc", "-f", $tmpDump)
& docker @dumpCmd
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump fallito."
}

$verifyCmd = @("exec")
if ($DbPassword) {
    $verifyCmd += @("-e", "PGPASSWORD=$DbPassword")
}
$verifyCmd += @($DockerContainer, "pg_restore", "-l", $tmpDump)
& docker @verifyCmd | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "pg_restore -l fallito: dump non valido."
}

& docker cp "$DockerContainer`:$tmpDump" $dumpPath
if ($LASTEXITCODE -ne 0) {
    throw "Copia dump da container fallita."
}

& docker exec $DockerContainer rm -f $tmpDump | Out-Null

$hash = (Get-FileHash -Path $dumpPath -Algorithm SHA256).Hash
Set-Content -Path $shaPath -Value "$hash  $dumpName" -Encoding UTF8

$cutoff = (Get-Date).AddDays(-$RetentionDays)
Get-ChildItem -Path $BackupRoot -File -Recurse |
    Where-Object { $_.LastWriteTime -lt $cutoff } |
    Remove-Item -Force
Get-ChildItem -Path $BackupRoot -Directory -Recurse |
    Where-Object { @(Get-ChildItem -Path $_.FullName -Force).Count -eq 0 } |
    Remove-Item -Force

Write-Log "Backup OK: $dumpPath"
Write-Log "SHA256: $hash"
