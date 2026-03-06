@echo off
echo Building Islands Runner image...

cd /d "C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server"
docker build -t transcript-islands -f islands_runner\Dockerfile .

if %errorlevel% neq 0 (
    echo Failed to build image. Check the output for errors.
    pause
    exit /b 1
)

echo Image built successfully.
echo Running islands build (reads DB from config/db_config.json)...
docker run --rm -v "%CD%\config:/app/config" transcript-islands %*

if %errorlevel% neq 0 (
    echo Islands run finished with errors.
) else (
    echo Islands run completed.
)

pause
