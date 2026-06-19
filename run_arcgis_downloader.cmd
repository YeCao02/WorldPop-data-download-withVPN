@echo off
setlocal
title WorldPop ArcGIS Downloader

call conda run --no-capture-output -n GEO python -u "%~dp0download_links.py" --backend arcgis %*
set "EXIT_CODE=%ERRORLEVEL%"

echo.
echo Downloader exited with code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
