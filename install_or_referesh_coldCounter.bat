@echo off
SETLOCAL

REM ==================================================
REM Check if Python is installed
REM ==================================================
python --version >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo Python is already installed.
) ELSE (
    echo Python not found. Installing Python 3.12 silently...

    REM Download Python installer
    set PYTHON_INSTALLER=%TEMP%\python-installer.exe
    powershell -Command "Invoke-WebRequest -Uri https://www.python.org/ftp/python/3.12.2/python-3.12.2-amd64.exe -OutFile '%PYTHON_INSTALLER%'"

    REM Install Python silently for all users and add to PATH
    %PYTHON_INSTALLER% /quiet InstallAllUsers=1 PrependPath=1 Include_test=0

    REM Wait a few seconds for install to finish
    timeout /t 10 >nul

    echo Python installation complete.
)

REM ==================================================
REM  Upgrade pip and install dependencies
REM ==================================================
echo Upgrading pip...
python -m pip install --upgrade pip

IF EXIST requirements.txt (
    echo Installing dependencies from requirements.txt...
    python -m pip install -r requirements.txt
) ELSE (
    echo No requirements.txt found. Skipping dependency install.
)

REM ==================================================
REM  Run the Python script
REM ==================================================
echo Running build_coldCounter.py...
cd /d %~dp0\code
python build_coldCounter.py

REM ==================================================
REM Keep terminal open
REM ==================================================
echo.
pause
ENDLOCAL