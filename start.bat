@echo off

REM --- 1. 切换到脚本所在目录 (我们已经知道这至关重要) ---
cd /d "%~dp0"

REM --- 2. 确保程序已编译 ---
REM    (部署前请手动运行一次 "cargo build --release")

REM --- 3. 使用原生、简单的 start /b 在后台启动程序 ---
REM    - start /b: 在当前控制台会话中启动程序，但不创建新窗口，也不等待它结束。
REM    - "": start 命令语法要求，在可执行文件路径前有一个空的标题。
REM    - "target\release\BinanceLocalServer.exe": 我们的目标程序。
REM    - >> start.log 2>&1: 由 cmd.exe 自身来处理日志重定向，这非常稳定。
start /b "" "target\release\BinanceLocalServer.exe" >> start.log 2>&1

REM 脚本会立即执行到这里并退出，cmd窗口一闪而过。