#!/bin/bash
# Task 4: Run FastAPI Application
# This script starts the FastAPI server

set -e

echo "=========================================="
echo "Task 4: Starting Medical Data Warehouse API"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please copy env.template to .env and configure it"
    exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "⚠️  Warning: Virtual environment not activated"
    echo "Activating virtual environment..."
    if [ -d "venv" ]; then
        source venv/bin/activate
    else
        echo "❌ Error: venv directory not found"
        echo "Please create a virtual environment first: python -m venv venv"
        exit 1
    fi
fi

# Check if required packages are installed
echo "Checking dependencies..."
python -c "import fastapi, uvicorn, sqlalchemy, pydantic" 2>/dev/null || {
    echo "❌ Error: Required packages not installed"
    echo "Please install dependencies: pip install -r requirements.txt"
    exit 1
}

# Get API host and port from environment or use defaults
API_HOST=${API_HOST:-0.0.0.0}
API_PORT=${API_PORT:-8000}

echo ""
echo "Starting FastAPI server..."
echo "Host: $API_HOST"
echo "Port: $API_PORT"
echo ""
echo "API Documentation will be available at:"
echo "  - Swagger UI: http://localhost:$API_PORT/docs"
echo "  - ReDoc: http://localhost:$API_PORT/redoc"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Start the server
uvicorn api.main:app --host "$API_HOST" --port "$API_PORT" --reload
