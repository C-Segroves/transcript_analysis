@echo off
REM Run one islands worker for multi-machine or multi-process runs.
REM Usage: run_islands_worker.bat <worker_index> <total_workers>
REM   e.g. run_islands_worker.bat 0 4   (worker 0 of 4)
REM On each machine, run with a different worker_index (0, 1, 2, 3 for 4 workers).
REM Optional: add --clear on worker 0 only to clear the table first (e.g. first run only).

setlocal
if "%~1"=="" (
    echo Usage: run_islands_worker.bat ^<worker_index^> ^<total_workers^>
    echo   e.g. run_islands_worker.bat 0 4
    exit /b 1
)
if "%~2"=="" (
    echo Usage: run_islands_worker.bat ^<worker_index^> ^<total_workers^>
    exit /b 1
)

set WORKER_INDEX=%~1
set TOTAL_WORKERS=%~2
shift
shift

cd /d "C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server"
echo Running islands worker %WORKER_INDEX% of %TOTAL_WORKERS%...
docker run --rm -v "%CD%\config:/app/config" transcript-islands --worker-index %WORKER_INDEX% --total-workers %TOTAL_WORKERS% %*
endlocal
