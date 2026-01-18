# Quick Start Guide - Task 1: Data Scraping

This guide will help you get started with Task 1 of the Medical Telegram Data Warehouse project.

## Prerequisites

- Python 3.10 or higher
- Telegram account
- Telegram API credentials (from https://my.telegram.org/apps)

## Step 1: Get Telegram API Credentials

1. Visit https://my.telegram.org/apps
2. Log in with your phone number
3. Create a new application
4. Copy your `api_id` and `api_hash`

## Step 2: Set Up the Project

### Option A: Using the Setup Script (Recommended)

```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
```

### Option B: Manual Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp env.template .env  # Or create manually
```

## Step 3: Configure Environment Variables

Edit the `.env` file with your credentials:

```env
TELEGRAM_API_ID=your_api_id_here
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+251912345678  # Your phone number with country code
```

## Step 4: Configure Channels to Scrape

Edit `src/config.py` to add or modify the channels you want to scrape:

```python
TELEGRAM_CHANNELS = [
    "lobelia4cosmetics",
    "tikvahpharma",
    # Add more channels here
]
```

**Note:** Channel names should be without `@` and without `t.me/`. For example:
- ✅ `lobelia4cosmetics` (correct)
- ❌ `@lobelia4cosmetics` (incorrect)
- ❌ `t.me/lobelia4cosmetics` (incorrect)

## Step 5: Run the Scraper

```bash
python src/scraper.py
```

On first run, Telegram will send you a verification code. Enter it when prompted.

## Step 6: Verify Results

After scraping, check:

1. **Raw JSON files**: `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`
2. **Downloaded images**: `data/raw/images/{channel_name}/{message_id}.jpg`
3. **Log files**: `logs/scraper_YYYYMMDD_HHMMSS.log`

## Troubleshooting

### "Rate limited" errors
- The scraper automatically handles rate limiting
- It will wait and retry automatically
- If you see frequent rate limits, reduce `SCRAPING_LIMIT` in `src/config.py`

### "Channel is private" errors
- Make sure the channel is public
- Some channels may require you to join them first
- Try accessing the channel in Telegram app first

### "Session password needed" error
- Your account has 2FA enabled
- Enter your 2FA password when prompted
- Or temporarily disable 2FA for testing

### Import errors
- Make sure you've activated the virtual environment
- Run `pip install -r requirements.txt` again
- Check that you're using Python 3.10+

## Data Lake Structure

After scraping, your data lake will look like:

```
data/
└── raw/
    ├── images/
    │   ├── lobelia4cosmetics/
    │   │   ├── 12345.jpg
    │   │   └── 12346.jpg
    │   └── tikvahpharma/
    │       └── 23456.jpg
    └── telegram_messages/
        ├── 2026-01-15/
        │   ├── lobelia4cosmetics.json
        │   └── tikvahpharma.json
        └── 2026-01-16/
            └── ...
```

## Next Steps

Once Task 1 is complete, you can proceed to:
- **Task 2**: Data Modeling and Transformation with dbt
- **Task 3**: Data Enrichment with YOLO
- **Task 4**: Build Analytical API
- **Task 5**: Pipeline Orchestration with Dagster

## Support

- Slack channel: #all-week8
- Office hours: Mon–Fri, 08:00–15:00 UTC
