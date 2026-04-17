@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

set "DO_CLEAN=0"
set "USE_LOCKED=0"

:parse_args
if "%~1"=="" goto after_parse
if /I "%~1"=="--clean" (
    set "DO_CLEAN=1"
    shift
    goto parse_args
)
if /I "%~1"=="--locked" (
    set "USE_LOCKED=1"
    shift
    goto parse_args
)

echo Unknown argument: %~1
exit /b 1

:after_parse
if "%DO_CLEAN%"=="1" (
    echo Cleaning target directory...
    cargo clean
    if errorlevel 1 exit /b 1
)

set "CARGO_CMD=cargo build --release"
if "%USE_LOCKED%"=="1" (
    set "CARGO_CMD=%CARGO_CMD% --locked"
)

echo Running: %CARGO_CMD%
call %CARGO_CMD%
if errorlevel 1 exit /b 1

set "ARTIFACT=%~dp0target\release\BinanceLocalServer.exe"
if not exist "%ARTIFACT%" (
    echo Release artifact not found: %ARTIFACT%
    exit /b 1
)

for %%I in ("%ARTIFACT%") do (
    echo Release build completed.
    echo Artifact: %%~fI
    echo Size: %%~zI bytes
)

exit /b 0
