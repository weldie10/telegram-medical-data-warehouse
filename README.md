# Medical Telegram Data Warehouse

An end-to-end data pipeline for Telegram medical channels, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.

## Project Overview

This project builds a robust data platform that generates actionable insights about Ethiopian medical businesses using data scraped from public Telegram channels.

## Project Structure

```
medical-telegram-warehouse/
├── .vscode/
│   └── settings.json
├── .github/
│   └── workflows/
│       └── unittests.yml
├── .env               # Secrets (API keys, DB passwords) - DO NOT COMMIT
├── env.template       # Environment variables template
├── .gitignore
├── docker-compose.yml  # Container orchestration
├── Dockerfile          # Python environment
├── .dockerignore       # Docker build exclusions
├── requirements.txt
├── README.md
├── data/
│   └── raw/
│       ├── images/
│       └── telegram_messages/
├── medical_warehouse/            # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
├── src/
│   ├── __init__.py
│   └── scraper.py
├── api/
│   ├── __init__.py
│   ├── main.py                   # FastAPI application
│   ├── database.py               # Database connection
│   └── schemas.py                # Pydantic models
├── notebooks/
│   └── __init__.py
├── tests/
│   └── __init__.py
└── scripts/
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd telegram-medical-data-warehouse
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

1. Copy `env.template` to `.env`:
   ```bash
   cp env.template .env
   ```

2. Get Telegram API credentials:
   - Visit https://my.telegram.org/apps
   - Create a new application
   - Copy your `api_id` and `api_hash`

3. Update `.env` with your credentials:
   ```
   TELEGRAM_API_ID=your_api_id_here
   TELEGRAM_API_HASH=your_api_hash_here
   TELEGRAM_PHONE=+251912345678
   ```

## Docker Setup (Alternative)

### Using Docker Compose

1. **Set up environment variables:**
   ```bash
   cp env.template .env
   # Edit .env with your credentials
   ```

2. **Build and start services:**
   ```bash
   # Start PostgreSQL and scraper
   docker-compose up -d postgres scraper
   
   # Start API (Task 4)
   docker-compose --profile api up -d api
   
   # Start Dagster (Task 5)
   docker-compose --profile dagster up -d dagster
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f scraper
   ```

4. **Stop services:**
   ```bash
   docker-compose down
   ```

### Using Dockerfile Only

```bash
# Build the image
docker build -t medical-warehouse .

# Run the scraper
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  medical-warehouse python src/scraper.py
```

## Task 1: Data Scraping and Collection

### Running the Scraper

```bash
python src/scraper.py
```

The scraper will:
- Extract messages from configured Telegram channels
- Download images to `data/raw/images/{channel_name}/{message_id}.jpg`
- Save raw JSON data to `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`
- Generate logs in `logs/` directory

### Channels Scraped

- CheMed Telegram Channel
- Lobelia Cosmetics (https://t.me/lobelia4cosmetics)
- Tikvah Pharma (https://t.me/tikvahpharma)
- Additional channels from https://et.tgstat.com/medicine

## Data Lake Structure

```
data/
└── raw/
    ├── images/
    │   └── {channel_name}/
    │       └── {message_id}.jpg
    └── telegram_messages/
        └── YYYY-MM-DD/
            └── {channel_name}.json
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

This project follows PEP 8 style guidelines.

## License

[Add your license here]
