# Medical Telegram Data Warehouse

End-to-end data pipeline for Telegram medical channels with dbt transformation, YOLOv8 image analysis, and FastAPI analytics.

## Quick Start

### Option 1: Run via Dagster Pipeline (Recommended)

```bash
# 1. Setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure
cp env.template .env
# Edit .env with your Telegram API credentials

# 3. Start Dagster UI
dagster dev -f pipeline.py

# 4. Access UI at http://localhost:3000 and run the pipeline
```

### Option 2: Run Manually

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
├── pipeline.py       # Dagster pipeline orchestration
└── data/raw/          # Raw data lake (gitignored)
```

## Features

- **Task 1**: Telegram channel scraping with image download
- **Task 2**: dbt star schema (dim_channels, dim_dates, fct_messages)
- **Task 3**: YOLOv8 image detection and classification
- **Task 4**: FastAPI analytical endpoints
- **Task 5**: Dagster pipeline orchestration with scheduling

## API Endpoints

- `GET /api/reports/top-products` - Most mentioned products
- `GET /api/channels/{name}/activity` - Channel activity trends
- `GET /api/search/messages?query=term` - Search messages
- `GET /api/reports/visual-content` - Image statistics
- `GET /docs` - API documentation

## Pipeline Orchestration (Task 5)

The pipeline is orchestrated using Dagster. See [DAGSTER_GUIDE.md](DAGSTER_GUIDE.md) for detailed instructions.

**Quick Start:**
```bash
dagster dev -f pipeline.py
```

Access the UI at http://localhost:3000 to:
- Run the pipeline manually
- Monitor execution and logs
- View the daily schedule (runs at 2 AM UTC)
- Track pipeline history

**Pipeline Operations:**
1. `scrape_telegram_data` - Scrapes Telegram channels
2. `load_raw_to_postgres` - Loads raw data to database
3. `run_yolo_enrichment` - Runs YOLO object detection
4. `run_dbt_transformations` - Executes dbt models

## Environment Variables

See `env.template` for required variables:
- `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`
- `POSTGRES_HOST` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`

## License

[Add your license here]
