"""
FastAPI application for Medical Data Warehouse Analytics API.

This API exposes analytical endpoints that query the data warehouse
built with dbt to answer business questions.
"""

import re
from collections import Counter
from datetime import date, datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import (
    TopProductsResponse,
    ProductItem,
    ChannelActivityResponse,
    DailyActivity,
    MessageSearchResponse,
    MessageResult,
    VisualContentStatsResponse,
    ChannelVisualStats,
    ErrorResponse,
)

# Initialize FastAPI app
app = FastAPI(
    title="Medical Data Warehouse API",
    description="""
    Analytical API for Medical Telegram Data Warehouse.
    
    This API provides endpoints to analyze data from Telegram medical channels,
    including message content, channel activity, visual content statistics, and more.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ============================================================================
# Helper Functions
# ============================================================================

def extract_product_terms(message_text: str) -> List[str]:
    """
    Extract potential product/medical terms from message text.
    
    This is a simple implementation that looks for:
    - Words in ALL CAPS (often product names)
    - Words with numbers (e.g., "Paracetamol 500mg")
    - Common medical/pharmaceutical patterns
    """
    if not message_text:
        return []
    
    # Split into words
    words = re.findall(r'\b\w+\b', message_text)
    
    # Filter for potential product terms
    terms = []
    for word in words:
        # Skip very short words
        if len(word) < 3:
            continue
        
        # Skip common stop words
        stop_words = {
            'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
            'from', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has',
            'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may',
            'might', 'can', 'this', 'that', 'these', 'those', 'a', 'an', 'as',
        }
        if word.lower() in stop_words:
            continue
        
        # Include words that are all caps (often product names)
        if word.isupper() and len(word) > 2:
            terms.append(word)
        # Include words with numbers
        elif re.search(r'\d', word):
            terms.append(word)
        # Include capitalized words (potential product names)
        elif word[0].isupper() and len(word) > 4:
            terms.append(word)
    
    return terms


# ============================================================================
# Endpoint 1: Top Products
# ============================================================================

@app.get(
    "/api/reports/top-products",
    response_model=TopProductsResponse,
    summary="Get Top Products/Terms",
    description="""
    Returns the most frequently mentioned terms/products across all channels.
    
    This endpoint analyzes message text to extract and count product mentions,
    returning the most common terms.
    """,
    tags=["Reports"],
)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Maximum number of products to return"),
    db: Session = Depends(get_db),
):
    """
    Get top products/terms mentioned across all channels.
    """
    try:
        # Query all messages with text
        query = text("""
            SELECT message_text
            FROM marts.fct_messages
            WHERE message_text IS NOT NULL
                AND LENGTH(TRIM(message_text)) > 0
        """)
        
        result = db.execute(query)
        messages = result.fetchall()
        
        # Extract terms from all messages
        all_terms = []
        for (message_text,) in messages:
            terms = extract_product_terms(message_text)
            all_terms.extend(terms)
        
        # Count term frequencies
        term_counter = Counter(all_terms)
        
        # Get top terms
        top_terms = term_counter.most_common(limit)
        
        # Get channels where each term appears
        products = []
        for term, frequency in top_terms:
            # Find channels where this term appears
            channel_query = text("""
                SELECT DISTINCT dc.channel_name
                FROM marts.fct_messages fm
                INNER JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                WHERE LOWER(fm.message_text) LIKE LOWER(:term)
            """)
            channel_result = db.execute(channel_query, {"term": f"%{term}%"})
            channels = [row[0] for row in channel_result.fetchall()]
            
            products.append(ProductItem(
                term=term,
                frequency=frequency,
                channels=channels,
            ))
        
        return TopProductsResponse(
            limit=limit,
            total_terms=len(term_counter),
            products=products,
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing top products: {str(e)}"
        )


# ============================================================================
# Endpoint 2: Channel Activity
# ============================================================================

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=ChannelActivityResponse,
    summary="Get Channel Activity",
    description="""
    Returns posting activity and trends for a specific channel.
    
    Includes daily breakdown of messages, views, and forwards.
    """,
    tags=["Channels"],
)
async def get_channel_activity(
    channel_name: str,
    db: Session = Depends(get_db),
):
    """
    Get activity statistics for a specific channel.
    """
    try:
        # Get channel information
        channel_query = text("""
            SELECT 
                channel_name,
                channel_type,
                total_posts,
                first_post_date,
                last_post_date,
                avg_views
            FROM marts.dim_channels
            WHERE channel_name = :channel_name
        """)
        
        channel_result = db.execute(channel_query, {"channel_name": channel_name})
        channel_row = channel_result.fetchone()
        
        if not channel_row:
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel_name}' not found"
            )
        
        # Get daily activity
        daily_query = text("""
            SELECT 
                dd.full_date as date,
                COUNT(fm.message_id) as message_count,
                SUM(fm.view_count) as total_views,
                AVG(fm.view_count) as avg_views,
                SUM(fm.forward_count) as total_forwards
            FROM marts.fct_messages fm
            INNER JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            INNER JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dc.channel_name = :channel_name
            GROUP BY dd.full_date
            ORDER BY dd.full_date DESC
        """)
        
        daily_result = db.execute(daily_query, {"channel_name": channel_name})
        daily_activities = []
        
        for row in daily_result.fetchall():
            daily_activities.append(DailyActivity(
                date=row[0],
                message_count=row[1],
                total_views=row[2],
                avg_views=float(row[3]) if row[3] else 0.0,
                total_forwards=row[4],
            ))
        
        return ChannelActivityResponse(
            channel_name=channel_row[0],
            channel_type=channel_row[1],
            total_posts=channel_row[2],
            first_post_date=channel_row[3],
            last_post_date=channel_row[4],
            avg_views=float(channel_row[5]) if channel_row[5] else 0.0,
            daily_activity=daily_activities,
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching channel activity: {str(e)}"
        )


# ============================================================================
# Endpoint 3: Message Search
# ============================================================================

@app.get(
    "/api/search/messages",
    response_model=MessageSearchResponse,
    summary="Search Messages",
    description="""
    Searches for messages containing a specific keyword.
    
    Returns matching messages with their content and engagement metrics.
    """,
    tags=["Search"],
)
async def search_messages(
    query: str = Query(..., min_length=2, description="Search keyword"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
    db: Session = Depends(get_db),
):
    """
    Search for messages containing a specific keyword.
    """
    try:
        # Search messages
        search_query = text("""
            SELECT 
                fm.message_id,
                dc.channel_name,
                fm.message_text,
                fm.message_date,
                fm.view_count,
                fm.forward_count,
                fm.has_image
            FROM marts.fct_messages fm
            INNER JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE LOWER(fm.message_text) LIKE LOWER(:query)
            ORDER BY fm.message_date DESC
            LIMIT :limit
        """)
        
        result = db.execute(
            search_query,
            {"query": f"%{query}%", "limit": limit}
        )
        
        messages = []
        for row in result.fetchall():
            messages.append(MessageResult(
                message_id=row[0],
                channel_name=row[1],
                message_text=row[2],
                message_date=row[3],
                view_count=row[4],
                forward_count=row[5],
                has_image=row[6],
            ))
        
        # Get total count (for pagination info)
        count_query = text("""
            SELECT COUNT(*)
            FROM marts.fct_messages fm
            WHERE LOWER(fm.message_text) LIKE LOWER(:query)
        """)
        
        count_result = db.execute(count_query, {"query": f"%{query}%"})
        total_found = count_result.scalar()
        
        return MessageSearchResponse(
            query=query,
            limit=limit,
            total_found=total_found,
            messages=messages,
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error searching messages: {str(e)}"
        )


# ============================================================================
# Endpoint 4: Visual Content Stats
# ============================================================================

@app.get(
    "/api/reports/visual-content",
    response_model=VisualContentStatsResponse,
    summary="Get Visual Content Statistics",
    description="""
    Returns statistics about image usage across channels.
    
    Includes breakdown by image category (promotional, product_display, lifestyle, other)
    and statistics per channel.
    """,
    tags=["Reports"],
)
async def get_visual_content_stats(
    db: Session = Depends(get_db),
):
    """
    Get visual content statistics across all channels.
    """
    try:
        # Check if fct_image_detections exists
        check_query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'marts' 
                AND table_name = 'fct_image_detections'
            )
        """)
        
        table_exists = db.execute(check_query).scalar()
        
        if not table_exists:
            # Return empty stats if table doesn't exist
            return VisualContentStatsResponse(
                total_images=0,
                channels=[],
                category_summary={
                    "promotional": 0,
                    "product_display": 0,
                    "lifestyle": 0,
                    "other": 0,
                },
            )
        
        # Get channel-level statistics
        channel_query = text("""
            SELECT 
                dc.channel_name,
                COUNT(fid.message_id) as total_images,
                COUNT(CASE WHEN fid.image_category = 'promotional' THEN 1 END) as promotional_count,
                COUNT(CASE WHEN fid.image_category = 'product_display' THEN 1 END) as product_display_count,
                COUNT(CASE WHEN fid.image_category = 'lifestyle' THEN 1 END) as lifestyle_count,
                COUNT(CASE WHEN fid.image_category = 'other' THEN 1 END) as other_count
            FROM marts.fct_image_detections fid
            INNER JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
            GROUP BY dc.channel_name
            ORDER BY total_images DESC
        """)
        
        channel_result = db.execute(channel_query)
        
        channels = []
        total_images = 0
        category_counts = {
            "promotional": 0,
            "product_display": 0,
            "lifestyle": 0,
            "other": 0,
        }
        
        for row in channel_result.fetchall():
            channel_name = row[0]
            total = row[1]
            promotional = row[2]
            product_display = row[3]
            lifestyle = row[4]
            other = row[5]
            
            total_images += total
            category_counts["promotional"] += promotional
            category_counts["product_display"] += product_display
            category_counts["lifestyle"] += lifestyle
            category_counts["other"] += other
            
            promotional_pct = (promotional / total * 100) if total > 0 else 0.0
            product_display_pct = (product_display / total * 100) if total > 0 else 0.0
            
            channels.append(ChannelVisualStats(
                channel_name=channel_name,
                total_images=total,
                promotional_count=promotional,
                product_display_count=product_display,
                lifestyle_count=lifestyle,
                other_count=other,
                promotional_percentage=round(promotional_pct, 2),
                product_display_percentage=round(product_display_pct, 2),
            ))
        
        return VisualContentStatsResponse(
            total_images=total_images,
            channels=channels,
            category_summary=category_counts,
        )
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching visual content stats: {str(e)}"
        )


# ============================================================================
# Health Check Endpoint
# ============================================================================

@app.get(
    "/health",
    summary="Health Check",
    description="Check if the API is running and can connect to the database.",
    tags=["Health"],
)
async def health_check(db: Session = Depends(get_db)):
    """
    Health check endpoint.
    """
    try:
        # Test database connection
        db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "database": "disconnected",
                "error": str(e),
            },
        )


# ============================================================================
# Root Endpoint
# ============================================================================

@app.get(
    "/",
    summary="API Root",
    description="Root endpoint with API information.",
    tags=["Root"],
)
async def root():
    """
    Root endpoint.
    """
    return {
        "name": "Medical Data Warehouse API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "top_products": "/api/reports/top-products",
            "channel_activity": "/api/channels/{channel_name}/activity",
            "search_messages": "/api/search/messages",
            "visual_content": "/api/reports/visual-content",
        },
    }
