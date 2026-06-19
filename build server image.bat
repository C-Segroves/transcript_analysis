@echo off
echo Building Server Image...

cd /d "%~dp0"
docker build -t my-server-image -f server\Dockerfile .

if %errorlevel% equ 0 (
    echo Server image built successfully!
    echo Creating network if it doesn't exist...
    docker network create transcript-network 2>nul
    echo Running Server Image...
    echo Dashboard will be available at http://localhost:8080
    docker run -d --name server-container --network transcript-network -p 5000:5000 -p 8080:8080 my-server-image
) else (
    echo Failed to build server image. Check the output for errors.
)

pause