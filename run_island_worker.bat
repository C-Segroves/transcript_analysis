@echo off
REM Build and run a DB-coordinated island worker.
REM
REM Usage:
REM   run_island_worker.bat                 - run a worker that polls forever
REM   run_island_worker.bat --refresh --once  - seed tasks from scores, drain queue, exit
REM   run_island_worker.bat --threshold 0.6 --min-length 8
REM
REM Run this on as many machines/containers as you like; each worker just claims
REM whatever (vid_id, model_id) tasks are still pending. The DB in
REM config/db_config.json must be reachable from each machine.

cd /d "%~dp0"

echo Building island worker image...
docker build -t transcript-island-worker -f island_worker\Dockerfile .
if %errorlevel% neq 0 (
    echo Failed to build image. Check the output for errors.
    pause
    exit /b 1
)

echo Image built. Running island worker...
REM First run? Seed the task table once with:  run_island_worker.bat --refresh --once
docker run --rm -v "%CD%\config:/app/config" transcript-island-worker %*

if %errorlevel% neq 0 (
    echo Island worker finished with errors.
) else (
    echo Island worker finished.
)

pause
