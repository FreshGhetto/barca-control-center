param(
    [Parameter(Mandatory = $true)]
    [string]$BackupFile,
    [string]$DockerContainer = "barca-postgres",
    [string]$DbName = "barca",
    [string]$DbUser = "barca_user",
    [string]$DbPassword = $env:BARCA_DB_PASSWORD
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $Message"
}

if (-not (Test-Path -Path $BackupFile -PathType Leaf)) {
    throw "Backup file non trovato: $BackupFile"
}

$backupAbs = (Resolve-Path $BackupFile).Path
$fileName = Split-Path -Path $backupAbs -Leaf
$tmpDump = "/tmp/$fileName"

Write-Log "Restore start: file=$backupAbs db=$DbName container=$DockerContainer"

$inspect = & docker inspect $DockerContainer 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Container Docker non trovato: $DockerContainer"
}

& docker cp $backupAbs "$DockerContainer`:$tmpDump"
if ($LASTEXITCODE -ne 0) {
    throw "Copia dump nel container fallita."
}

$envArgs = @()
if ($DbPassword) {
    $envArgs = @("-e", "PGPASSWORD=$DbPassword")
}

$terminateSql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DbName' AND pid <> pg_backend_pid();"
& docker exec @envArgs $DockerContainer psql -U $DbUser -d postgres -v ON_ERROR_STOP=1 -c $terminateSql | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Terminazione connessioni attive fallita."
}

& docker exec @envArgs $DockerContainer dropdb -U $DbUser --if-exists $DbName
if ($LASTEXITCODE -ne 0) {
    throw "dropdb fallito."
}

& docker exec @envArgs $DockerContainer createdb -U $DbUser $DbName
if ($LASTEXITCODE -ne 0) {
    throw "createdb fallito."
}

& docker exec @envArgs $DockerContainer pg_restore -U $DbUser -d $DbName --clean --if-exists --no-owner $tmpDump
if ($LASTEXITCODE -ne 0) {
    throw "pg_restore fallito."
}

& docker exec $DockerContainer rm -f $tmpDump | Out-Null

Write-Log "Restore OK: db=$DbName from $backupAbs"
