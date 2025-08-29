@echo off
setlocal enabledelayedexpansion

title Service Controller

:menu
cls
echo =========================================
echo           Service Controller
echo =========================================
echo.
echo 1. Start all services
echo 2. Stop all services  
echo 3. Show running services
echo 4. Exit
echo.
set /p choice="Choose an option (1-4): "

if "%choice%"=="1" goto start_services
if "%choice%"=="2" goto stop_services
if "%choice%"=="3" goto show_services
if "%choice%"=="4" goto exit
echo Invalid choice. Please try again.
timeout /t 2 >nul
goto menu

:start_services
cls
echo Starting services...

rem --- Redis ---
echo Starting Redis...
start "MyApp-Redis" cmd /k "redis-server --port 6380 --slaveof 127.0.0.1 6379 || (echo Redis stopped unexpectedly. Press any key to close... && pause >nul && exit)"
timeout /t 3 >nul

rem --- Worker ---
echo Starting Worker...
start "MyApp-Worker" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate && (celery -A celery_app worker --loglevel=info --pool=threads || (echo Worker stopped. Press any key to close... && pause >nul)) && exit"
timeout /t 3 >nul

rem --- Beat ---
echo Starting Beat...
start "MyApp-Beat" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate && (celery -A celery_app beat --loglevel=info || (echo Beat stopped. Press any key to close... && pause >nul)) && exit"
timeout /t 3 >nul

rem --- FastApi App ---
echo Starting FastAPI...
start "MyApp-FastAPI" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate && (fastapi run main.py || (echo FastAPI stopped. Press any key to close... && pause >nul)) && exit"

echo.
echo All services started successfully!
echo Press any key to return to menu...
pause >nul
goto menu

:stop_services
cls
echo Stopping all services...

rem Create a PowerShell command to close windows by title more reliably
powershell -Command "Get-Process | Where-Object {$_.MainWindowTitle -like '*MyApp-*'} | ForEach-Object { $_.CloseMainWindow(); Start-Sleep -Milliseconds 500; if (!$_.HasExited) { $_.Kill() } }" >nul 2>&1

rem Backup method: Kill by process and port
echo Cleaning up remaining processes...

rem Kill Redis on specific port
for /f "tokens=5" %%i in ('netstat -ano 2^>nul ^| findstr ":6380"') do (
    taskkill /pid %%i /f >nul 2>&1
)

rem Kill Celery processes
taskkill /im celery.exe /f >nul 2>&1

rem Kill FastAPI processes on port 8000
for /f "tokens=5" %%i in ('netstat -ano 2^>nul ^| findstr ":8000"') do (
    taskkill /pid %%i /f >nul 2>&1
)

echo All services stopped.
echo Press any key to return to menu...
pause >nul
goto menu

:show_services
cls
echo Current running services:
echo.

echo Redis processes:
tasklist /fi "imagename eq redis-server.exe" 2>nul | findstr redis-server.exe

echo.
echo Python/Celery processes:
tasklist /fi "imagename eq python.exe" 2>nul | findstr python.exe
tasklist /fi "imagename eq celery.exe" 2>nul | findstr celery.exe

echo.
echo Ports in use:
netstat -ano | findstr ":6380\|:8000" 2>nul

echo.
echo Press any key to return to menu...
pause >nul
goto menu

:exit
echo Exiting...
exit /b 0