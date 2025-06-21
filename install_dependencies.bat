@echo off
chcp 65001 > nul
cd /d "%~dp0"
echo ===================================
echo   PDF Archive Database Manager
echo   Установка зависимостей
echo ===================================
echo.
echo Проверка версии Python...
python --version
if errorlevel 1 (
    echo ОШИБКА: Python не найден!
    echo Установите Python с https://www.python.org/downloads/windows/
    echo Не забудьте поставить галочку "Add Python to PATH"
    pause
    exit /b 1
)
echo.
echo Проверка pip...
pip --version
if errorlevel 1 (
    echo ОШИБКА: pip не найден!
    pause
    exit /b 1
)
echo.
echo Установка зависимостей...
pip install -r requirements.txt
if errorlevel 1 (
    echo ОШИБКА при установке зависимостей!
    pause
    exit /b 1
)
echo.
echo ✅ Зависимости успешно установлены!
echo Теперь можно запускать:
echo - run_indexing.bat (для создания базы данных)
echo - run_search.bat (для поиска)
echo.
pause 