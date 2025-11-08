@echo off
REM --- 这个脚本用于诊断，它会显示所有错误并暂停 ---
cd /d "%~dp0"

echo --- DEBUG MODE ---
echo Current directory: %cd%
echo Executing PowerShell command...
echo --------------------------------------------------

REM --- 执行和 start.bat 相同的命令，但不隐藏窗口，也不重定向日志 ---
REM --- 这样所有的输出（包括错误）都会直接显示在屏幕上 ---
powershell -NoProfile -ExecutionPolicy Bypass -Command "& { Start-Process -FilePath 'target\release\BinanceLocalServer.exe' -WorkingDirectory '%~dp0' -Wait }"

echo --------------------------------------------------
echo Command finished.
echo Press any key to exit.
pause