@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ===================================
echo   PDF Archive Database Manager
echo   Поиск по базе данных
echo ===================================
echo.
python ../search.py
echo.
echo Нажмите любую клавишу для выхода...
pause > nul 