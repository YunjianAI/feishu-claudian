@echo off
REM Stop the silently-running Feishu bot.
REM Only kills the python.exe whose command line contains main.py (this bot),
REM so other Python programs are left untouched.
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -like '*main.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }; Write-Host 'Feishu bot stopped.'"
echo.
echo Done. You can close this window.
timeout /t 3 >nul
