@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "LAUNCHER=%~dp0scripts\launcher.py"

where py >nul 2>nul
if errorlevel 1 goto use_python

py -3.12 -c "import sys" >nul 2>nul
if errorlevel 1 goto use_py_any

py -3.12 "%LAUNCHER%" %*
goto done

:use_py_any
py "%LAUNCHER%" %*
goto done

:use_python
where python >nul 2>nul
if errorlevel 1 (
    echo [LicitaLead] Python nao encontrado no PATH.
    echo Instale o Python 3.12 em: https://www.python.org/downloads/windows/
    pause
    exit /b 1
)
python "%LAUNCHER%" %*
goto done

:done
set "exit_code=%errorlevel%"
if not "%exit_code%"=="0" (
    echo.
    echo [LicitaLead] Nao foi possivel iniciar ^(codigo %exit_code%^). Leia as mensagens acima.
    pause
)
exit /b %exit_code%
