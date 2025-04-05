#!/bin/bash
echo "Building Client Image..."

cd /path/to/transcript_analysis_server
docker build -t my-client-image -f client/Dockerfile .

if [ $? -eq 0 ]; then
    echo "Client image built successfully!"
    echo "Running Client Image..."
    docker run -it --name client-container -e SERVER_HOST=<WINDOWS_IP> my-client-image
else
    echo "Failed to build client image. Check the output for errors."
    exit 1
fi