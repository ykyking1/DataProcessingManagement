<#
.SYNOPSIS
    Watches the HOST's (Windows) real free memory and runs
    "wsl --shutdown" automatically when it drops to a critical level.

.DESCRIPTION
    The memory guards inside scripts/load_extended_telemetry.py (pre-
    check, part-count throttle, query watchdog) run INSIDE the
    dpm-services container and read /proc/meminfo -- that is the shared
    Docker Desktop / WSL2 VM's own view, which is NOT identical to the
    HOST's (Windows) real free memory. Proven 2026-08-28: host free
    memory dropped from 1.8GB to 0.5GB while the in-container watchdog
    never triggered, because the container's own view still reported
    "enough" memory.

    This script watches what the container CANNOT see -- the host's
    real free memory -- directly from Windows, and applies the only
    real fix (wsl --shutdown) from the host side when the container
    cannot rescue itself. Run it IN PARALLEL with
    load_extended_telemetry.py (or any other heavy Docker/WSL2 job).

    Turkish diacritics are intentionally avoided in all log/console
    text -- Windows PowerShell 5.1 misparses UTF-8 .ps1 source files
    without a BOM, which corrupted an earlier version of this script's
    own log messages. Plain ASCII avoids that failure mode entirely.

.PARAMETER CriticalFreeMemoryGB
    "wsl --shutdown" triggers below this value. Default 1.0GB.

.PARAMETER PollIntervalSeconds
    Check interval in seconds. Default 2s -- fast enough to catch even
    the sharpest drops seen today (~1GB within a few seconds).

.PARAMETER StopFlagPath
    Creating a file at this path makes the watchdog exit cleanly
    (without running wsl --shutdown) -- use this to stop the watchdog
    once a load finishes successfully.

.EXAMPLE
    # Start in the background:
    Start-Process powershell -ArgumentList '-NoProfile','-File','scripts\watch_host_memory.ps1' -WindowStyle Hidden

    # Stop cleanly once the load finishes successfully:
    New-Item -ItemType File -Path "$env:TEMP\host_memory_watchdog.stop" -Force
#>

param(
    [double]$CriticalFreeMemoryGB = 1.0,
    [int]$PollIntervalSeconds = 2,
    [string]$LogPath = "$PSScriptRoot\..\raporlar\host_memory_watchdog_log.txt",
    [string]$StopFlagPath = "$env:TEMP\host_memory_watchdog.stop"
)

function Get-FreeMemoryGB {
    (Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1MB
}

function Write-Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $Message"
    Write-Host $line
    Add-Content -Path $LogPath -Value $line -Encoding ascii
}

# Clear any leftover stop signal from a previous run -- otherwise the
# watchdog would exit immediately on start.
if (Test-Path $StopFlagPath) {
    Remove-Item $StopFlagPath -Force
}

Write-Log ("Host memory watchdog started (threshold: {0}GB, poll interval: {1}s, stop flag: {2})." -f $CriticalFreeMemoryGB, $PollIntervalSeconds, $StopFlagPath)

while ($true) {
    if (Test-Path $StopFlagPath) {
        Write-Log "Stop signal received -- watchdog shutting down cleanly."
        Remove-Item $StopFlagPath -Force -ErrorAction SilentlyContinue
        break
    }

    $freeGB = [math]::Round((Get-FreeMemoryGB), 2)

    if ($freeGB -lt $CriticalFreeMemoryGB) {
        Write-Log ("CRITICAL: free memory {0}GB < {1}GB threshold -- running 'wsl --shutdown'." -f $freeGB, $CriticalFreeMemoryGB)
        wsl --shutdown
        Start-Sleep -Seconds 2
        $afterGB = [math]::Round((Get-FreeMemoryGB), 2)
        Write-Log ("'wsl --shutdown' completed (free memory now: {0}GB). Watchdog stopping -- WSL/Docker containers are down too; restart them (docker start ...) before restarting the watchdog." -f $afterGB)
        break
    }

    Start-Sleep -Seconds $PollIntervalSeconds
}
