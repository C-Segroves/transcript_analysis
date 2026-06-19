@echo off
echo Building Client Image...

cd /d "%~dp0"
docker build -t my-client-image -f client\Dockerfile .

REM Set SERVER_HOST to your task-server IP before running, e.g.:  set SERVER_HOST=your-server-ip
if "%SERVER_HOST%"=="" (
    echo ERROR: set SERVER_HOST to your task-server IP first ^(set SERVER_HOST=^<ip^>^).
    pause
    exit /b 1
)

if %errorlevel% equ 0 (
    echo Client image built successfully!
    echo Creating network if it doesn't exist...
    docker network create transcript-network 2>nul
    echo Running Client Image...
    setlocal EnableDelayedExpansion
    for /f "tokens=*" %%i in ('hostname') do set HOST_NAME=%%i
    echo Hostname: !HOST_NAME!
    docker run -it --name client-container --network transcript-network -e SERVER_HOST=%SERVER_HOST% -e MACHINE_NAME=!HOST_NAME! my-client-image
    endlocal
) else (
    echo Failed to build client image. Check the output for errors.
)

pause