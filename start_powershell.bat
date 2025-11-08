@echo off

REM --- 让 cmd.exe 调用 PowerShell，并让 PowerShell 以隐藏模式启动你的程序 ---
powershell.exe -WindowStyle Hidden -Command "Start-Process -FilePath '%~dp0target\release\BinanceLocalServer.exe' -WorkingDirectory '%~dp0' -RedirectStandardOutput '%~dp0start.log' -RedirectStandardError '%~dp0start.log' -Append"