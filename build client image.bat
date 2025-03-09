@echo off
echo Building Client Image...

cd /d C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server
docker build -t my-client-image -f client\Dockerfile .

if %errorlevel% equ 0 (
    echo Client image built successfully!
    echo Creating network if it doesn't exist...
    docker network create transcript-network 2>nul
    echo Running Client Image...
    docker run -it --name client-container --network transcript-network -e SERVER_HOST=server-container my-client-image
) else (
    echo Failed to build client image. Check the output for errors.
)

pause