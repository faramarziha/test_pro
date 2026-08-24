@echo off
REM ============================================
REM ProxyHub — Build standalone Windows .exe
REM ============================================
REM Requirements: Python 3.10+ with pip
REM
REM Run this script from the project folder.
REM Output will be in: dist\ProxyHub.exe
REM ============================================

echo.
echo ==================================
echo  ProxyHub EXE Builder
echo ==================================
echo.

REM Step 1: Install build dependencies
echo [1/3] Installing build dependencies...
pip install pyinstaller --quiet
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install PyInstaller.
    pause
    exit /b 1
)

REM Step 2: Install project dependencies
echo [2/3] Installing project requirements...
pip install -r requirements.txt --quiet
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to install requirements.
    pause
    exit /b 1
)

REM Step 3: Build the exe
echo [3/3] Building ProxyHub.exe...
pyinstaller proxyhub.spec --clean --noconfirm
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Build failed.
    pause
    exit /b 1
)

echo.
echo ==================================
echo  BUILD SUCCESS!
echo  EXE: dist\ProxyHub.exe
echo ==================================
echo.
echo To run the app, double-click:
echo   dist\ProxyHub.exe
echo.
echo Then open your browser to:
echo   http://localhost:8501
echo.
pause