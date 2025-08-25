@echo off
rem --- Redis ---
start "Redis"  cmd /k "redis-server --port 6380 --slaveof 127.0.0.1 6379"

timeout /t 2 >nul

rem --- Worker ---
start "Worker" cmd /k "cd /d "%~dp0" && call venv\Scripts\activate && celery -A celery_app worker --loglevel=info --pool=threads"

timeout /t 2 >nul

rem --- Beat ---
start "Beat"   cmd /k "cd /d "%~dp0" && call venv\Scripts\activate && celery -A celery_app beat --loglevel=info"