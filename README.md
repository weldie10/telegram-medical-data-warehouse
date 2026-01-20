# Medical Telegram Data Warehouse

End-to-end data pipeline for Telegram medical channels with dbt transformation, YOLOv8 image analysis, and FastAPI analytics.

## Quick Start

```bash
# 1. Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp env.template .env
# Edit .env with your Telegram API credentials

# 3. Scrape data
python src/scraper.py

# 4. Load to database
python scripts/load_raw_to_postgres.py

# 5. Transform with dbt
cd medical_warehouse
dbt deps
dbt run

# 6. Run API
uvicorn api.main:app --reload
```

## Project Structure

```
├── src/              # Telegram scraper
├── api/              # FastAPI analytics endpoints
├── medical_warehouse/ # dbt models (star schema)
├── scripts/          # Data loading and utilities
└── data/raw/          # Raw data lake (gitignored)
```

## Features

- **Task 1**: Telegram channel scraping with image download
- **Task 2**: dbt star schema (dim_channels, dim_dates, fct_messages)
- **Task 3**: YOLOv8 image detection and classification
- **Task 4**: FastAPI analytical endpoints

## API Endpoints

- `GET /api/reports/top-products` - Most mentioned products
- `GET /api/channels/{name}/activity` - Channel activity trends
- `GET /api/search/messages?query=term` - Search messages
- `GET /api/reports/visual-content` - Image statistics
- `GET /docs` - API documentation

## Environment Variables

See `env.template` for required variables:
- `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`
- `POSTGRES_HOST` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`

## License

[Add your license here]
