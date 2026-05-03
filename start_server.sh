#!/bin/bash
echo "Starting Smart Connect backend..."

# Script location
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Detect virtual environment python (assuming it will be in server/.venv on Linux)
if [ -f "$DIR/.venv/bin/python" ]; then
    VENV_PYTHON="$DIR/.venv/bin/python"
else
    echo "Warning: .venv not found in $DIR, trying system python..."
    VENV_PYTHON="python"
fi

# Run uvicorn. 
# We use 'main:app' because we are already in the directory where main.py is.
"$VENV_PYTHON" -m uvicorn main:app --host 0.0.0.0 --port 1488 --reload
