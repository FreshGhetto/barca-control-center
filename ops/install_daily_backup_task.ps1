param(
    [string]$TaskName = "BARCA_DB_DailyBackup",
    [string]$RunAt = "02:30",
    [string]$BackupRoot = "C:\BARCA\backups",
    [string]$DockerContainer = "barca-postgres",
    [string]$DbName = "barca",
    [string]$DbUser = "barca_user",
    [int]$RetentionDays = 30
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $PSScriptRoot "backup_barca.ps1"
if (-not (Test-Path -Path $scriptPath -PathType Leaf)) {
    throw "Script backup non trovato: $scriptPath"
}

$taskCmd = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -BackupRoot `"$BackupRoot`" -DockerContainer `"$DockerContainer`" -DbName `"$DbName`" -DbUser `"$DbUser`" -RetentionDays $RetentionDays"

$null = cmd /c "schtasks /Query /TN `"$TaskName`" >nul 2>&1"
if ($LASTEXITCODE -eq 0) {
    & schtasks.exe /Delete /TN $TaskName /F | Out-Null
}

& schtasks.exe /Create `
    /TN $TaskName `
    /TR $taskCmd `
    /SC DAILY `
    /ST $RunAt `
    /F | Out-Null

Write-Host "Scheduled task installata: $TaskName alle $RunAt"
Write-Host "Comando: $taskCmd"
