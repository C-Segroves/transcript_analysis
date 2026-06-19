#!/bin/bash
echo "Building Client Image..."

# Task-server host: pass as arg 1, or set SERVER_HOST, or it falls back to the
# "host" field in config/db_config.json.
cd "$(dirname "$0")"
SERVER_HOST=${1:-${SERVER_HOST:-$(sed -n 's/.*"host"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' config/db_config.json 2>/dev/null | head -n1)}}
if [ -z "$SERVER_HOST" ]; then
    echo "ERROR: set SERVER_HOST (or pass the server IP as arg 1)."
    exit 1
fi
export SERVER_HOST
export MACHINE_NAME=$(hostname)

docker build -t my-client-image -f client/Dockerfile .

if [ $? -eq 0 ]; then
    echo "Client image built successfully!"
    echo "Running Client Image..."
    docker run -it --name client-container -e SERVER_HOST=$SERVER_HOST -e MACHINE_NAME=$MACHINE_NAME my-client-image
else
    echo "Failed to build client image. Check the output for errors."
    exit 1
fi

# Print the machine name and pause for a keystroke
echo "Machine Name: $MACHINE_NAME"
echo "Press any key to exit..."
read -n 1 -s