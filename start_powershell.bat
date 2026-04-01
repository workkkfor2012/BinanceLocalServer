@echo off
setlocal EnableExtensions
cd /d "%~dp0"

for /f %%I in ('powershell -NoProfile -Command "Get-Process BinanceLocalServer -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty Id"') do set "EXISTING_PID=%%I"
if defined EXISTING_PID (
    echo [LocalServer] BinanceLocalServer already running ^(PID %EXISTING_PID%^). Skipping duplicate start.
    exit /b 0
)

for %%P in (6001 6002 6003 30000) do (
    for /f %%I in ('powershell -NoProfile -Command "Get-NetTCPConnection -State Listen -LocalPort %%P -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty LocalPort"') do set "BLOCKED_PORT=%%I"
    if defined BLOCKED_PORT goto :port_in_use
)

goto :start_server

:port_in_use
echo [LocalServer] Required port %BLOCKED_PORT% is already in use. Check the existing listener before starting another instance.
exit /b 1

:start_server

echo [LocalServer] Starting BinanceLocalServer.exe in hidden PowerShell...
powershell.exe -NoProfile -WindowStyle Hidden -Command "& '%~dp0target\release\BinanceLocalServer.exe' >> '%~dp0start.log' 2>&1"
