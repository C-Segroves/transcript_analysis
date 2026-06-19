@echo off
REM ============================================================================
REM launch_fleet.bat -- build and launch this machine's workers (Windows).
REM
REM Starts N model-major scoring clients (one per CPU by default) plus island
REM workers, all pointed at the task server. Containers auto-restart on reboot.
REM
REM Quick start on a fresh machine:
REM   1) install Docker Desktop
REM   2) git clone <repo> && cd transcript_analysis
REM   3) create config\db_config.json   (gitignored, so copy it onto the machine)
REM   4) launch_fleet.bat
REM
REM Usage:
REM   launch_fleet.bat [num_clients] [num_island_workers]
REM   set SERVER_HOST=<server-ip> & launch_fleet.bat       (override the task-server host)
REM   set SEED=1 & launch_fleet.bat                         (ALSO seed islands)
REM
REM Run "set SEED=1" on exactly ONE machine, ONE time, to populate
REM island_task_table from existing scores. Other machines just run the bat.
REM ============================================================================
setlocal enabledelayedexpansion
cd /d "%~dp0"

set CLIENTS=%1
if "%CLIENTS%"=="" set CLIENTS=%NUMBER_OF_PROCESSORS%
set ISLANDS=%2
if "%ISLANDS%"=="" set ISLANDS=1
set HOST=%COMPUTERNAME%

if not exist config\db_config.json (
    echo ERROR: config\db_config.json not found. Copy it onto this machine first.
    exit /b 1
)

REM Task-server host: use SERVER_HOST if set, else the "host" from config\db_config.json
REM (task server and DB share a machine in this setup).
if "%SERVER_HOST%"=="" (
    for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "(ConvertFrom-Json (Get-Content config\db_config.json -Raw)).host"`) do set "SERVER_HOST=%%i"
)
if "%SERVER_HOST%"=="" (
    echo ERROR: could not determine SERVER_HOST. Set it ^(set SERVER_HOST=^<ip^>^) or add "host" to config\db_config.json.
    exit /b 1
)

echo ^>^> Building images (scoring client + island worker)...
docker build -t transcript-client -f client\Dockerfile . || exit /b 1
docker build -t transcript-island-worker -f island_worker\Dockerfile . || exit /b 1

REM Note: config is baked into the images at build time (the Dockerfiles COPY
REM config\db_config.json), and we rebuild every run, so no -v mount is needed.
REM Avoiding -v also sidesteps Windows drive-letter/colon volume-path issues.
if "%SEED%"=="1" (
    echo ^>^> Seeding island tasks from existing scores (one-time)...
    docker run --rm --entrypoint python transcript-island-worker setup_island_tables.py --refresh
)

echo ^>^> Launching %CLIENTS% scoring client(s) + %ISLANDS% island worker(s) -^> server %SERVER_HOST%
for /L %%i in (1,1,%CLIENTS%) do (
    docker rm -f %HOST%-client-%%i >nul 2>&1
    docker run -d --restart unless-stopped --name %HOST%-client-%%i -e SERVER_HOST=%SERVER_HOST% -e MACHINE_NAME=%HOST%-c%%i -e MODEL_CACHE_SIZE=1 transcript-client
)

for /L %%i in (1,1,%ISLANDS%) do (
    docker rm -f %HOST%-island-%%i >nul 2>&1
    docker run -d --restart unless-stopped --name %HOST%-island-%%i -e WORKER_NAME=%HOST%-isl%%i transcript-island-worker
)

echo ^>^> Running. Use "docker ps" to view, "docker logs -f %HOST%-client-1" to tail.
echo ^>^> Stop all on this box:  for /f %%n in ('docker ps -q --filter name^=%HOST%-') do docker rm -f %%n
docker ps --filter "name=%HOST%-" --format "   {{.Names}}  {{.Status}}"
