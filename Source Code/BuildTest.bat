@echo off
setlocal EnableDelayedExpansion

REM ─────────────────────────────────────────────────────────────────────────────
REM Stream Connector — Build & Sign Script for PyInstaller + SignTool
REM Python 3.11 required
REM ─────────────────────────────────────────────────────────────────────────────

:: ─── Configuration ───────────────────────────────────────────────────────────
set "APP_NAME=Stream Connector"
set "EXE_NAME=Stream Connector.exe"

set "DIST_ROOT=dist"
set "DEBUG_DIR=%DIST_ROOT%\debug"
set "RELEASE_DIR=%DIST_ROOT%\release"

set "ICON_PATH=assets\logo.ico"
set "CERT_THUMBPRINT=3F087DBEF105477A8EDFFDB23696E9338771E028"
set "TIMESTAMP_URL=http://timestamp.digicert.com"
set "INSTALLER_DIR=installer"
set "SAVED_DIR=saved"
set "PYTHONWARNINGS=ignore::UserWarning:pkg_resources"

:: ─── Check Python 3.11 ───────────────────────────────────────────────────────
echo Checking Python 3.11…
py -3.11 --version >nul 2>&1 || (
    echo [ERROR] Python 3.11 is not installed.
    pause & exit /b 1
)

:: ─── Build RELEASE ───────────────────────────────────────────────────────────
echo.
echo Building RELEASE version...

rd /s /q "%RELEASE_DIR%" 2>nul


py -3.11 -m PyInstaller ^
    --onefile ^
    --windowed ^
    --noupx ^
    --clean ^
    --name "%APP_NAME%" ^
    --icon "%ICON_PATH%" ^
    --distpath "%RELEASE_DIR%" ^
    --workpath build\release ^
    --version-file version_info.txt ^
    --manifest manifest.xml ^
    --add-data "saved;saved" ^
    --add-data "assets;assets" ^
    --hidden-import clr ^
    --hidden-import System ^
    --hidden-import System.Reflection ^
    --hidden-import pythonnet ^
    --hidden-import asyncio ^
    --hidden-import asyncio.subprocess ^
    --hidden-import asyncio.futures ^
    --hidden-import threading ^
    --hidden-import queue ^
    --hidden-import concurrent ^
    --hidden-import concurrent.futures ^
    --hidden-import tkinter ^
    --hidden-import tkinter.ttk ^
    --hidden-import tkinter.filedialog ^
    --hidden-import tkinter.simpledialog ^
    --hidden-import tkinter.messagebox ^
    --hidden-import PIL ^
    --hidden-import PIL.Image ^
    --hidden-import PIL.ImageTk ^
    --hidden-import websockets ^
    --hidden-import websockets.legacy.client ^
    --hidden-import websockets.legacy.server ^
    --hidden-import aiohttp ^
    --hidden-import flask ^
    --hidden-import requests ^
    --hidden-import ssl ^
    --hidden-import socket ^
    --hidden-import pythonosc ^
    --hidden-import pythonosc.dispatcher ^
    --hidden-import pythonosc.osc_server ^
    --hidden-import pythonosc.udp_client ^
    --hidden-import TikTokLive ^
    --hidden-import TikTokLive.client ^
    --hidden-import TikTokLive.events ^
    --hidden-import TikTokLive.types ^
	--hidden-import zeroconf ^
	--hidden-import zeroconf._utils ^
	--hidden-import zeroconf._utils.ipaddress ^
	--hidden-import zeroconf._cache ^
	--hidden-import zeroconf._dns ^
	--hidden-import zeroconf._engine ^
	--hidden-import zeroconf._listener ^
	--hidden-import zeroconf._protocol ^
	--hidden-import zeroconf._services ^
	--hidden-import zeroconf._services.browser ^
	--hidden-import zeroconf._services.info ^
	--hidden-import zeroconf._handlers ^
	--hidden-import zeroconf._exceptions ^
	--hidden-import zeroconf.const ^
	--hidden-import zeroconf.logger ^
	--hidden-import zeroconf.serviceinfo ^
	--hidden-import zeroconf.version ^
	--collect-all zeroconf ^
    --hidden-import watchdog ^
    --hidden-import watchdog.observers ^
    --hidden-import watchdog.observers.winapi ^
    --hidden-import watchdog.observers.winapi_buffer ^
    --hidden-import watchdog.events ^
    dashboard.py || (
        echo [ERROR] Release build failed.
        pause & exit /b 1
    )

:: ─── Locate SignTool ─────────────────────────────────────────────────────────
echo.
echo Locating SignTool…
set "SIGNTOOL_PATH=C:\Program Files (x86)\Windows Kits\10\bin\10.0.22621.0\x64\signtool.exe"
for /f "tokens=* usebackq" %%A in (`where signtool 2^>nul`) do set "SIGNTOOL_PATH=%%A"
if not exist "!SIGNTOOL_PATH!" (
    echo [ERROR] SignTool.exe not found.
    pause & exit /b 1
)

:: ─── Sign EXE ────────────────────────────────────────────────────────────────
set "EXE_PATH=%RELEASE_DIR%\%APP_NAME%.exe"
echo Signing EXE: "%EXE_PATH%"

if exist "%EXE_PATH%" (
    "!SIGNTOOL_PATH!" sign ^
        /fd SHA256 ^
        /td SHA256 ^
        /tr "%TIMESTAMP_URL%" ^
        /sha1 %CERT_THUMBPRINT% ^
        /s My ^
        /d "%APP_NAME%" ^
        "%EXE_PATH%" || (
            echo [ERROR] EXE signing failed.
            pause & exit /b 1
        )
    echo ✅ EXE signed.
) else (
    echo [ERROR] EXE not found.
    pause & exit /b 1
)

:: ─── Done ────────────────────────────────────────────────────────────────────
echo.
echo ✅ ALL DONE.
echo RELEASE EXE  : "%EXE_PATH%"
echo.
pause