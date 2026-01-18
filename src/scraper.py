"""
Telegram Channel Scraper for Medical Data Warehouse

This script scrapes messages from public Telegram channels related to
Ethiopian medical businesses and stores them in a raw data lake structure.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

# Add parent directory to path for imports when running as script
BASE_DIR = Path(__file__).parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import colorlog
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UsernameNotOccupiedError,
    SessionPasswordNeededError,
)
from telethon.tl.types import Message, MessageMediaPhoto

try:
    from src.config import TELEGRAM_CHANNELS, SCRAPING_LIMIT
except ImportError:
    # Fallback if config module is not available
    TELEGRAM_CHANNELS = [
        "lobelia4cosmetics",
        "tikvahpharma",
    ]
    SCRAPING_LIMIT = None

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

# Base directories (already set above for imports)
DATA_DIR = BASE_DIR / "data" / "raw"
IMAGES_DIR = DATA_DIR / "images"
MESSAGES_DIR = DATA_DIR / "telegram_messages"
LOGS_DIR = BASE_DIR / "logs"

# Telegram channels to scrape (imported from config)
CHANNELS = TELEGRAM_CHANNELS

# Session file location
SESSION_FILE = BASE_DIR / "telegram_session.session"


def setup_logging() -> colorlog.StreamHandler:
    """Configure colored logging for the scraper."""
    # Create logs directory if it doesn't exist
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = colorlog.getLogger("telegram_scraper")
    logger.setLevel(colorlog.DEBUG)
    
    # Console handler with colors
    console_handler = colorlog.StreamHandler(sys.stdout)
    console_handler.setLevel(colorlog.INFO)
    console_format = colorlog.ColoredFormatter(
        "%(log_color)s%(levelname)s%(reset)s - %(message)s",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = LOGS_DIR / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = colorlog.StreamHandler(open(log_file, "w"))
    file_handler.setLevel(colorlog.DEBUG)
    file_format = colorlog.ColoredFormatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger


def ensure_directories():
    """Create necessary directories if they don't exist."""
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    MESSAGES_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


def extract_message_data(message: Message, channel_name: str) -> Dict:
    """
    Extract relevant data from a Telegram message.
    
    Args:
        message: Telethon Message object
        channel_name: Name of the channel
        
    Returns:
        Dictionary containing message data
    """
    message_data = {
        "message_id": message.id,
        "channel_name": channel_name,
        "message_date": message.date.isoformat() if message.date else None,
        "message_text": message.text or "",
        "has_media": message.media is not None,
        "image_path": None,
        "views": message.views if hasattr(message, "views") else None,
        "forwards": message.forwards if hasattr(message, "forwards") else None,
        "reply_to_msg_id": message.reply_to_msg_id if hasattr(message, "reply_to_msg_id") else None,
        "is_reply": message.is_reply if hasattr(message, "is_reply") else False,
    }
    
    return message_data


async def download_image(
    client: TelegramClient,
    message: Message,
    channel_name: str,
    logger: colorlog.Logger,
) -> Optional[str]:
    """
    Download image from a message if it contains a photo.
    
    Args:
        client: Telethon client instance
        message: Message object containing the image
        channel_name: Name of the channel
        logger: Logger instance
        
    Returns:
        Path to downloaded image or None
    """
    if not message.media or not isinstance(message.media, MessageMediaPhoto):
        return None
    
    try:
        # Create channel-specific image directory
        channel_image_dir = IMAGES_DIR / channel_name
        channel_image_dir.mkdir(parents=True, exist_ok=True)
        
        # Download image
        image_path = channel_image_dir / f"{message.id}.jpg"
        
        # Skip if already downloaded
        if image_path.exists():
            logger.debug(f"Image {image_path.name} already exists, skipping download")
            return str(image_path.relative_to(BASE_DIR))
        
        await client.download_media(message.media, file=str(image_path))
        logger.info(f"Downloaded image: {image_path.name}")
        
        return str(image_path.relative_to(BASE_DIR))
    
    except Exception as e:
        logger.error(f"Error downloading image for message {message.id}: {str(e)}")
        return None


async def scrape_channel(
    client: TelegramClient,
    channel_username: str,
    logger: colorlog.Logger,
    limit: Optional[int] = SCRAPING_LIMIT,
) -> List[Dict]:
    """
    Scrape messages from a Telegram channel.
    
    Args:
        client: Telethon client instance
        channel_username: Username of the channel (without @)
        logger: Logger instance
        limit: Maximum number of messages to scrape (None for all)
        
    Returns:
        List of message dictionaries
    """
    messages_data = []
    
    try:
        logger.info(f"Starting to scrape channel: {channel_username}")
        
        # Get channel entity
        entity = await client.get_entity(channel_username)
        channel_name = entity.title if hasattr(entity, "title") else channel_username
        logger.info(f"Channel title: {channel_name}")
        
        # Scrape messages
        message_count = 0
        async for message in client.iter_messages(entity, limit=limit):
            try:
                # Extract message data
                message_data = extract_message_data(message, channel_name)
                
                # Download image if present
                if message_data["has_media"]:
                    image_path = await download_image(client, message, channel_name, logger)
                    message_data["image_path"] = image_path
                
                messages_data.append(message_data)
                message_count += 1
                
                if message_count % 50 == 0:
                    logger.info(f"Scraped {message_count} messages from {channel_name}")
            
            except Exception as e:
                logger.error(f"Error processing message {message.id}: {str(e)}")
                continue
        
        logger.info(f"Successfully scraped {len(messages_data)} messages from {channel_name}")
        
    except ChannelPrivateError:
        logger.error(f"Channel {channel_username} is private or you don't have access")
    except UsernameNotOccupiedError:
        logger.error(f"Channel {channel_username} does not exist")
    except FloodWaitError as e:
        logger.warning(f"Rate limited. Waiting {e.seconds} seconds...")
        await asyncio.sleep(e.seconds)
        # Retry after waiting
        return await scrape_channel(client, channel_username, logger, limit)
    except Exception as e:
        logger.error(f"Error scraping channel {channel_username}: {str(e)}")
    
    return messages_data


def save_messages_to_data_lake(
    messages: List[Dict],
    channel_name: str,
    logger: colorlog.Logger,
):
    """
    Save scraped messages to the data lake in partitioned JSON format.
    
    Args:
        messages: List of message dictionaries
        channel_name: Name of the channel
        logger: Logger instance
    """
    if not messages:
        logger.warning(f"No messages to save for channel {channel_name}")
        return
    
    # Group messages by date
    messages_by_date = {}
    for msg in messages:
        if msg["message_date"]:
            date_str = msg["message_date"][:10]  # Extract YYYY-MM-DD
            if date_str not in messages_by_date:
                messages_by_date[date_str] = []
            messages_by_date[date_str].append(msg)
    
    # Save messages partitioned by date
    for date_str, date_messages in messages_by_date.items():
        date_dir = MESSAGES_DIR / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Sanitize channel name for filename
        safe_channel_name = channel_name.replace(" ", "_").replace("/", "_")
        json_file = date_dir / f"{safe_channel_name}.json"
        
        # Load existing data if file exists
        existing_data = []
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Could not parse existing JSON file: {json_file}")
        
        # Merge with existing data (avoid duplicates)
        existing_ids = {msg["message_id"] for msg in existing_data}
        new_messages = [
            msg for msg in date_messages if msg["message_id"] not in existing_ids
        ]
        
        if new_messages:
            all_messages = existing_data + new_messages
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(all_messages, f, ensure_ascii=False, indent=2)
            
            logger.info(
                f"Saved {len(new_messages)} new messages to {json_file} "
                f"(total: {len(all_messages)})"
            )
        else:
            logger.debug(f"No new messages to save for {date_str}")


async def main():
    """Main function to orchestrate the scraping process."""
    logger = setup_logging()
    ensure_directories()
    
    # Validate environment variables
    if not API_ID or not API_HASH:
        logger.error(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env file. "
            "Get them from https://my.telegram.org/apps"
        )
        sys.exit(1)
    
    if not PHONE:
        logger.error(
            "TELEGRAM_PHONE must be set in .env file (e.g., +251912345678)"
        )
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Telegram Medical Data Scraper")
    logger.info("=" * 60)
    logger.info(f"Channels to scrape: {', '.join(CHANNELS)}")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Images directory: {IMAGES_DIR}")
    logger.info(f"Messages directory: {MESSAGES_DIR}")
    
    # Initialize Telegram client
    client = TelegramClient(str(SESSION_FILE), int(API_ID), API_HASH)
    
    try:
        await client.start(phone=PHONE)
        logger.info("Successfully connected to Telegram")
        
        # Scrape each channel
        all_scraped_channels = []
        for channel in CHANNELS:
            try:
                messages = await scrape_channel(client, channel, logger)
                if messages:
                    # Get channel name from first message
                    channel_name = messages[0]["channel_name"]
                    save_messages_to_data_lake(messages, channel_name, logger)
                    all_scraped_channels.append(channel_name)
                else:
                    logger.warning(f"No messages scraped from {channel}")
            
            except Exception as e:
                logger.error(f"Failed to scrape channel {channel}: {str(e)}")
                continue
        
        # Summary
        logger.info("=" * 60)
        logger.info("Scraping Summary")
        logger.info("=" * 60)
        logger.info(f"Successfully scraped {len(all_scraped_channels)} channels")
        for channel in all_scraped_channels:
            logger.info(f"  - {channel}")
        logger.info("=" * 60)
    
    except SessionPasswordNeededError:
        logger.error(
            "Two-factor authentication is enabled. Please enter your password "
            "when prompted, or disable 2FA for this session."
        )
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
    
    finally:
        await client.disconnect()
        logger.info("Disconnected from Telegram")


if __name__ == "__main__":
    asyncio.run(main())
