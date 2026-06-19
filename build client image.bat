@echo off
echo Building Client Image...

cd /d "%~dp0"
docker build -t my-client-image -f client\Dockerfile .

REM Task-server host: use SERVER_HOST if set, else read "host" from config\db_config.json.
if "%SERVER_HOST%"=="" (
    for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "(ConvertFrom-Json (Get-Content config\db_config.json -Raw)).host"`) do set "SERVER_HOST=%%i"
)
if "%SERVER_HOST%"=="" (
    echo ERROR: could not determine SERVER_HOST. Set it ^(set SERVER_HOST=^<ip^>^) or add "host" to config\db_config.json.
    pause
    exit /b 1
)
echo Using SERVER_HOST=%SERVER_HOST%

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