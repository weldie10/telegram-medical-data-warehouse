"""
Pydantic schemas for request/response validation.
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# ============================================================================
# Top Products Endpoint Schemas
# ============================================================================

class ProductItem(BaseModel):
    """A product/term with its frequency."""
    term: str = Field(..., description="Product name or term")
    frequency: int = Field(..., description="Number of times the term appears")
    channels: List[str] = Field(..., description="List of channels where term appears")


class TopProductsResponse(BaseModel):
    """Response for top products endpoint."""
    limit: int = Field(..., description="Number of results returned")
    total_terms: int = Field(..., description="Total unique terms found")
    products: List[ProductItem] = Field(..., description="List of top products")


# ============================================================================
# Channel Activity Endpoint Schemas
# ============================================================================

class DailyActivity(BaseModel):
    """Daily posting activity for a channel."""
    date: date = Field(..., description="Date of activity")
    message_count: int = Field(..., description="Number of messages posted")
    total_views: int = Field(..., description="Total views for the day")
    avg_views: float = Field(..., description="Average views per message")
    total_forwards: int = Field(..., description="Total forwards for the day")


class ChannelActivityResponse(BaseModel):
    """Response for channel activity endpoint."""
    channel_name: str = Field(..., description="Name of the channel")
    channel_type: str = Field(..., description="Type of channel (Pharmaceutical, Cosmetics, etc.)")
    total_posts: int = Field(..., description="Total number of posts")
    first_post_date: date = Field(..., description="Date of first post")
    last_post_date: date = Field(..., description="Date of most recent post")
    avg_views: float = Field(..., description="Average views per post")
    daily_activity: List[DailyActivity] = Field(..., description="Daily activity breakdown")


# ============================================================================
# Message Search Endpoint Schemas
# ============================================================================

class MessageResult(BaseModel):
    """A single message search result."""
    message_id: int = Field(..., description="Message identifier")
    channel_name: str = Field(..., description="Channel where message was posted")
    message_text: str = Field(..., description="Text content of the message")
    message_date: datetime = Field(..., description="When the message was posted")
    view_count: int = Field(..., description="Number of views")
    forward_count: int = Field(..., description="Number of forwards")
    has_image: bool = Field(..., description="Whether message has an image")


class MessageSearchResponse(BaseModel):
    """Response for message search endpoint."""
    query: str = Field(..., description="Search query used")
    limit: int = Field(..., description="Maximum number of results returned")
    total_found: int = Field(..., description="Total number of matching messages")
    messages: List[MessageResult] = Field(..., description="List of matching messages")


# ============================================================================
# Visual Content Stats Endpoint Schemas
# ============================================================================

class ChannelVisualStats(BaseModel):
    """Visual content statistics for a channel."""
    channel_name: str = Field(..., description="Name of the channel")
    total_images: int = Field(..., description="Total number of images")
    promotional_count: int = Field(..., description="Number of promotional images")
    product_display_count: int = Field(..., description="Number of product display images")
    lifestyle_count: int = Field(..., description="Number of lifestyle images")
    other_count: int = Field(..., description="Number of other images")
    promotional_percentage: float = Field(..., description="Percentage of promotional images")
    product_display_percentage: float = Field(..., description="Percentage of product display images")


class VisualContentStatsResponse(BaseModel):
    """Response for visual content stats endpoint."""
    total_images: int = Field(..., description="Total images across all channels")
    channels: List[ChannelVisualStats] = Field(..., description="Statistics per channel")
    category_summary: dict = Field(..., description="Summary by image category")


# ============================================================================
# Error Response Schema
# ============================================================================

class ErrorResponse(BaseModel):
    """Error response schema."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Additional error details")
