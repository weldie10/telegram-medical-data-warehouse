"""
Unit tests for the Telegram scraper.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.scraper import (
    extract_message_data,
    save_messages_to_data_lake,
    ensure_directories,
)


class TestScraper:
    """Test cases for scraper functionality."""

    def test_extract_message_data(self):
        """Test message data extraction."""
        # Create a mock message
        mock_message = MagicMock()
        mock_message.id = 12345
        mock_message.date = None
        mock_message.text = "Test message"
        mock_message.media = None
        mock_message.views = 100
        mock_message.forwards = 5
        mock_message.reply_to_msg_id = None
        mock_message.is_reply = False

        result = extract_message_data(mock_message, "test_channel")

        assert result["message_id"] == 12345
        assert result["channel_name"] == "test_channel"
        assert result["message_text"] == "Test message"
        assert result["has_media"] is False
        assert result["views"] == 100
        assert result["forwards"] == 5

    def test_save_messages_to_data_lake(self, tmp_path):
        """Test saving messages to data lake structure."""
        # Mock BASE_DIR to use temp directory
        with patch("src.scraper.BASE_DIR", tmp_path):
            with patch("src.scraper.MESSAGES_DIR", tmp_path / "data" / "raw" / "telegram_messages"):
                messages = [
                    {
                        "message_id": 1,
                        "channel_name": "test_channel",
                        "message_date": "2026-01-15T10:00:00",
                        "message_text": "Test message 1",
                        "has_media": False,
                        "views": 100,
                        "forwards": 5,
                    },
                    {
                        "message_id": 2,
                        "channel_name": "test_channel",
                        "message_date": "2026-01-15T11:00:00",
                        "message_text": "Test message 2",
                        "has_media": False,
                        "views": 200,
                        "forwards": 10,
                    },
                ]

                logger = MagicMock()
                save_messages_to_data_lake(messages, "test_channel", logger)

                # Check if file was created
                json_file = tmp_path / "data" / "raw" / "telegram_messages" / "2026-01-15" / "test_channel.json"
                assert json_file.exists()

                # Verify content
                with open(json_file, "r") as f:
                    saved_data = json.load(f)
                    assert len(saved_data) == 2
                    assert saved_data[0]["message_id"] == 1
                    assert saved_data[1]["message_id"] == 2

    def test_ensure_directories(self, tmp_path):
        """Test directory creation."""
        with patch("src.scraper.BASE_DIR", tmp_path):
            with patch("src.scraper.IMAGES_DIR", tmp_path / "data" / "raw" / "images"):
                with patch("src.scraper.MESSAGES_DIR", tmp_path / "data" / "raw" / "telegram_messages"):
                    with patch("src.scraper.LOGS_DIR", tmp_path / "logs"):
                        ensure_directories()

                        assert (tmp_path / "data" / "raw" / "images").exists()
                        assert (tmp_path / "data" / "raw" / "telegram_messages").exists()
                        assert (tmp_path / "logs").exists()
