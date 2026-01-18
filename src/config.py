"""
Configuration file for Telegram channels and scraper settings.
"""

# Telegram channels to scrape
# Format: channel username without @ or t.me/
TELEGRAM_CHANNELS = [
    "lobelia4cosmetics",  # https://t.me/lobelia4cosmetics
    "tikvahpharma",  # https://t.me/tikvahpharma
    # Add more channels here
    # You can find more channels at: https://et.tgstat.com/medicine
    # Examples:
    # "chemed",  # If you find the CheMed channel username
    # "channel_name_here",
]

# Scraping settings
SCRAPING_LIMIT = None  # Set to None to scrape all messages, or an integer to limit
SCRAPING_DELAY = 1  # Delay between requests in seconds (to avoid rate limiting)

# Data lake settings
DATA_LAKE_BASE = "data/raw"
IMAGES_SUBDIR = "images"
MESSAGES_SUBDIR = "telegram_messages"
