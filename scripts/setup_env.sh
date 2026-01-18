#!/bin/bash
# Setup script for the medical telegram data warehouse project

set -e

echo "Setting up Medical Telegram Data Warehouse..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cat > .env << EOF
# Telegram API Credentials
# Get these from https://my.telegram.org/apps
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here

# Telegram Phone Number (with country code, e.g., +251912345678)
TELEGRAM_PHONE=your_phone_number_here

# Database Configuration (for later tasks)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=medical_warehouse
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
EOF
    echo "Please edit .env file with your Telegram API credentials"
else
    echo ".env file already exists"
fi

# Create necessary directories
echo "Creating data directories..."
mkdir -p data/raw/images
mkdir -p data/raw/telegram_messages
mkdir -p logs

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Telegram API credentials"
echo "2. Get API credentials from https://my.telegram.org/apps"
echo "3. Run: python src/scraper.py"
