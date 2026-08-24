@echo off
REM ============================================
REM ProxyHub — Quick launcher (no exe needed)
REM ============================================

echo.
echo ==================================
echo  ProxyHub - Proxy Intelligence Hub
echo ==================================
echo.

REM Check if requirements are installed
pip show streamlit >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Installing dependencies...
    pip install -r requirements.txt --quiet
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to install dependencies.
        echo Try: pip install -r requirements.txt
        pause
        exit /b 1
    )
    echo Done.
)

REM Clear old logs
if exist "%USERPROFILE%\.proxyhub\logs\proxyhub.log" (
    del "%USERPROFILE%\.proxyhub\logs\proxyhub.log" 2>nul
)

echo Starting ProxyHub...
echo.
echo Open your browser to: http://localhost:8501
echo.
echo Log file: %USERPROFILE%\.proxyhub\logs\proxyhub.log
echo Press Ctrl+C to stop.
echo.

streamlit run app.py --server.address=0.0.0.0 --server.port=8501 --server.headless=true

pause