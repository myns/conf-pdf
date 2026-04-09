@echo off
echo Installing dependencies...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] pip install failed. Make sure Python is installed and in PATH.
    pause
    exit /b 1
)
echo.
echo Done. Run with: python exporter.py
pause
