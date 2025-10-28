@echo off

:: Set the Docker image name
set IMAGE_NAME=shiny-server-app

:: Print the current directory for debugging
cd /d C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server
echo Current directory: %cd%

:: Build the Docker image (set the build context to the parent directory)
echo Building the Docker image...
docker build -t %IMAGE_NAME% -f shiny_server_app/Dockerfile .

:: Check if the build was successful
if %ERRORLEVEL% neq 0 (
    echo Docker build failed. Exiting.
    pause
    exit /b %ERRORLEVEL%
)

:: Run the Docker container
echo Running the Docker container...
docker run -p 8000:8000 --name %IMAGE_NAME% --rm %IMAGE_NAME%

:: Check if the container ran successfully
if %ERRORLEVEL% neq 0 (
    echo Docker container failed to start. Exiting.
    pause
    exit /b %ERRORLEVEL%
)

pause