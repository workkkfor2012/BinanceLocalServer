@echo off

REM 切换到脚本文件所在的目录，这对于确保能找到 target\release 下的程序至关重要
cd /d "%~dp0"

REM 使用 PowerShell 的 Start-Process 命令以隐藏窗口模式启动我们的服务器程序
REM -NoProfile: 加快 PowerShell 启动速度，不加载用户配置文件
REM -WindowStyle Hidden: 这是实现无窗口运行的关键
REM -Command "...": 执行指定的命令
REM '& { ... }': 在 PowerShell 中执行一个脚本块，是调用外部程序的标准做法
powershell.exe -NoProfile -WindowStyle Hidden -Command "& { Start-Process -FilePath 'target\release\BinanceLocalServer.exe'  }"