@echo off
chcp 65001 >nul
title Feishu Claudian Bot
cd /d D:\feishu-claude-code
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
echo ================================================
echo   Feishu Claudian Bot RESTARTING...
echo   waiting for old process to exit (5s)
echo ================================================
timeout /t 5 /nobreak >nul
echo   starting new instance...
echo   Keep this window OPEN  = bot ONLINE
echo.
.venv\Scripts\python.exe -u main.py
echo.
echo === bot stopped, press any key to close ===
pause >nul
