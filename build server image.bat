@echo off
echo Building Server Image...

cd /d C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server
docker build -t my-server-image -f server\Dockerfile .

if %errorlevel% equ 0 (
    echo Server image built successfully!
    echo Creating network if it doesn't exist...
    docker network create transcript-network 2>nul
    echo Running Server Image...
    docker run -d --name server-container --network transcript-network -p 5000:5000 my-server-image
) else (
    echo Failed to build server image. Check the output for errors.
)

pause