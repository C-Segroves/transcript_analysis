# filepath: /c:/Users/Chris/Desktop/git/transcript analysis/transcript_analysis_server/build client image.sh
#!/bin/bash
echo "Building Client Image..."

# Set the SERVER_HOST environment variable (passed as an argument or default to a known IP)
SERVER_HOST=${1:-192.168.1.108}  # Replace with the actual server IP if known
export SERVER_HOST
export MACHINE_NAME=$(hostname)

cd /path/to/transcript_analysis_server
docker build -t my-client-image -f client/Dockerfile .

if [ $? -eq 0 ]; then
    echo "Client image built successfully!"
    echo "Running Client Image..."
    docker run -it --name client-container -e SERVER_HOST=$SERVER_HOST -e MACHINE_NAME=$MACHINE_NAME my-client-image
else
    echo "Failed to build client image. Check the output for errors."
    exit 1
fi