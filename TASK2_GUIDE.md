# Task 2: Data Modeling and Transformation Guide

## Overview

Task 2 transforms raw, messy Telegram data into a clean, structured data warehouse using dbt and dimensional modeling (star schema).

## Architecture

```
Raw Data (JSON) 
    ↓
PostgreSQL raw.telegram_messages
    ↓
dbt Staging Layer (staging.stg_telegram_messages)
    ↓
dbt Marts Layer (Star Schema)
    ├── dim_channels
    ├── dim_dates
    └── fct_messages
```

## Step-by-Step Execution

### 1. Prerequisites

Ensure you have:
- ✅ Completed Task 1 (raw data in `data/raw/telegram_messages/`)
- ✅ PostgreSQL running (via Docker or local)
- ✅ Environment variables configured in `.env`

### 2. Load Raw Data to PostgreSQL

```bash
python scripts/load_raw_to_postgres.py
```

**What this does:**
- Creates `raw` schema
- Creates `raw.telegram_messages` table with proper schema
- Loads all JSON files from data lake
- Handles duplicates using UNIQUE constraint
- Shows loading statistics

**Expected output:**
```
==================================================
Loading Raw Data to PostgreSQL
==================================================
✓ Connected to PostgreSQL
✓ Raw schema created/verified
✓ Raw table created/verified

Loading JSON files from data/raw/telegram_messages...
Found X JSON files to process
  Loaded Y messages from channel_name.json
...

✓ Loaded Z total messages from JSON files

Loading Z messages to raw.telegram_messages...
  Loaded Z/Z messages...
✓ Successfully loaded Z messages to raw.telegram_messages

==================================================
Data Warehouse Statistics
==================================================
Total Messages: Z
Unique Channels: N
Date Range: YYYY-MM-DD to YYYY-MM-DD
==================================================
```

### 3. Install dbt Packages

```bash
cd medical_warehouse
dbt deps
```

This installs `dbt_utils` package used for surrogate keys and additional tests.

### 4. Verify dbt Connection

```bash
dbt debug
```

This verifies:
- ✅ Profile configuration
- ✅ Database connection
- ✅ dbt installation

### 5. Run dbt Transformations

**Build staging models:**
```bash
dbt run --select staging
```

**Build marts (dimensions and facts):**
```bash
dbt run --select marts
```

**Or build everything:**
```bash
dbt run
```

### 6. Run Tests

```bash
dbt test
```

This runs:
- Column-level tests (unique, not_null, relationships)
- Custom data quality tests
- Referential integrity tests

### 7. Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

Opens documentation at `http://localhost:8080` showing:
- Model DAG (lineage graph)
- Column descriptions
- Test results
- Data profiling

## Star Schema Design

### Dimension Tables

#### `dim_channels`
- **Purpose**: Channel information and statistics
- **Key**: `channel_key` (surrogate key)
- **Attributes**:
  - `channel_name`: Name of channel
  - `channel_type`: Classification (Pharmaceutical, Cosmetics, Medical, Other)
  - `first_post_date`, `last_post_date`: Date range
  - `total_posts`: Count of messages
  - `avg_views`: Average engagement
  - `image_percentage`: % of messages with images

#### `dim_dates`
- **Purpose**: Date dimension for time-based analysis
- **Key**: `date_key` (YYYYMMDD format)
- **Attributes**:
  - `full_date`: Date value
  - `day_of_week`, `day_name`: Day information
  - `week_of_year`, `month`, `month_name`: Time periods
  - `quarter`, `year`: Higher-level time periods
  - `is_weekend`: Business logic flag

### Fact Table

#### `fct_messages`
- **Purpose**: One row per message
- **Keys**:
  - `message_id`: Natural key
  - `channel_key`: FK to `dim_channels`
  - `date_key`: FK to `dim_dates`
- **Measures**:
  - `message_text`, `message_length`: Content
  - `view_count`, `forward_count`: Engagement
  - `has_image`: Media flag

## Data Quality Tests

### Built-in Tests

Located in `models/marts/schema.yml`:
- **Primary Keys**: `unique`, `not_null` on `channel_key`, `date_key`
- **Foreign Keys**: `relationships` test on `fct_messages.channel_key` → `dim_channels.channel_key`
- **Data Validation**: `accepted_values` for `channel_type`
- **Range Checks**: `accepted_range` for counts and averages

### Custom Tests

Located in `tests/`:
1. **`assert_no_future_messages.sql`**: Ensures no messages have future dates
2. **`assert_positive_views.sql`**: Ensures view counts are non-negative
3. **`assert_valid_date_range.sql`**: Validates date ranges are reasonable

## Querying the Star Schema

### Example Queries

**Top 10 channels by post count:**
```sql
SELECT 
    channel_name,
    channel_type,
    total_posts,
    avg_views
FROM marts.dim_channels
ORDER BY total_posts DESC
LIMIT 10;
```

**Daily posting trends:**
```sql
SELECT 
    dd.full_date,
    dd.day_name,
    COUNT(fm.message_id) as message_count,
    AVG(fm.view_count) as avg_views
FROM marts.fct_messages fm
JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
GROUP BY dd.full_date, dd.day_name
ORDER BY dd.full_date DESC;
```

**Channel engagement analysis:**
```sql
SELECT 
    dc.channel_name,
    dc.channel_type,
    COUNT(fm.message_id) as total_messages,
    AVG(fm.view_count) as avg_views,
    SUM(CASE WHEN fm.has_image THEN 1 ELSE 0 END) as messages_with_images
FROM marts.fct_messages fm
JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
GROUP BY dc.channel_name, dc.channel_type
ORDER BY avg_views DESC;
```

## Troubleshooting

### "Connection refused" error
- Check PostgreSQL is running: `docker-compose ps`
- Verify `.env` has correct database credentials
- Test connection: `psql -h localhost -U postgres -d medical_warehouse`

### "Schema does not exist" error
- Run `load_raw_to_postgres.py` first to create `raw` schema
- Or manually: `CREATE SCHEMA IF NOT EXISTS raw;`

### dbt tests failing
- Check test output for specific failures
- Review data quality issues in raw data
- Some tests may need adjustment based on your data

### "Package not found" error
- Run `dbt deps` to install packages
- Check `packages.yml` is present

## Next Steps

After completing Task 2:
- ✅ Star schema is built and tested
- ✅ Data is cleaned and structured
- ✅ Ready for Task 3 (YOLO enrichment) and Task 4 (API)
