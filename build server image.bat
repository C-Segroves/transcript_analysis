@echo off
echo Building Server Image...

cd /d C:\Users\Chris\Desktop\git\transcript analysis\transcript_analysis_server
docker build -t my-server-image -f server\Dockerfile .

if %errorlevel% equ 0 (
    echo Server image built successfully!
) else (
    echo Failed to build server image. Check the output for errors.
)

pause