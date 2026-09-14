@echo off
:: nuke_mata.bat — kill anything on port 9874, then start mata.py

:: Find and kill process on port 9874
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":9874 " ^| findstr "LISTENING"') do (
    echo Killing PID %%a on port 9874...
    taskkill /PID %%a /F >nul 2>&1
)

:: Small delay to let port release
ping 127.0.0.1 -n 3 >nul

:: Launch mata
start "" /B python "%~dp0mata.py"
