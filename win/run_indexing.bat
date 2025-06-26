@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ===================================
echo   PDF Archive Database Manager
echo   Создание базы данных
echo ===================================
echo.
echo ПРИМЕРЫ ПУТЕЙ ДЛЯ WINDOWS:
echo.
echo Путь к ZIP-архивам:
echo   C:\Users\%USERNAME%\Documents\PDFs
echo   D:\Архивы\PDF
echo   C:\Downloads\Documents
echo.
echo Путь к базе данных (можно оставить пустым):
echo   C:\Users\%USERNAME%\Documents\archive.db
echo   D:\Databases\archive.db
echo   (или просто нажмите Enter для текущей папки)
echo.
echo ===================================
echo.
python ../filling_db.py
echo.
echo Нажмите любую клавишу для выхода...
pause > nul 