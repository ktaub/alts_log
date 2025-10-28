@echo off
cd /d "%~dp0"
echo Checking Python virtual environment...

REM Check if venv exists, if not create it
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo Failed to create virtual environment. Make sure Python is installed.
        pause
        exit /b 1
    )
    echo Virtual environment created successfully.
)

REM Activate the virtual environment
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo Failed to activate virtual environment.
    pause
    exit /b 1
)

REM Check if requirements are installed by checking for pandas
python -c "import pandas" 2>nul
if errorlevel 1 (
    echo Installing required packages...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo Failed to install requirements.
        pause
        exit /b 1
    )
    echo Requirements installed successfully.
)

REM Run the application
echo Running Alts Log application...
python code\app.py > output.log 2>&1

REM Deactivate virtual environment
deactivate

pause 