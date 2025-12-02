@echo off
setlocal ENABLEDELAYEDEXPANSION

rem Resolve script directory
set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

set "PYTHON_CMD="
set "PYTHON_ARGS="

if defined PYTHON_BIN (
	set "PYTHON_CMD=%PYTHON_BIN%"
	goto :after_python_check
)

for %%P in (python3 python py) do (
	where %%P >nul 2>nul
	if not errorlevel 1 (
		set "PYTHON_CMD=%%P"
		if /I "%%P"=="py" set "PYTHON_ARGS=-3"
		goto :after_python_check
	)
)

:after_python_check
if not defined PYTHON_CMD (
	echo Python interpreter not found. Please install Python 3 or set PYTHON_BIN.>&2
	exit /b 1
)

if defined SCHEDULER_DB_PATH (
	set "DB_PATH=%SCHEDULER_DB_PATH%"
) else (
	set "DB_PATH=%SCRIPT_DIR%\scheduler.db"
)

if defined SCHEDULER_HOST (
	set "HOST=%SCHEDULER_HOST%"
) else (
	set "HOST=0.0.0.0"
)

if defined SCHEDULER_PORT (
	set "PORT=%SCHEDULER_PORT%"
) else (
	set "PORT=28256"
)

echo Starting scheduler on %HOST%:%PORT% (db=%DB_PATH%)
"%PYTHON_CMD%" %PYTHON_ARGS% "%SCRIPT_DIR%\scheduler_service.py" --host "%HOST%" --port "%PORT%" --db "%DB_PATH%"

endlocal
