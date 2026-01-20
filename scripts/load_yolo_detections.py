#!/usr/bin/env python3
"""
Load YOLO detection results CSV into PostgreSQL raw schema.

This script:
1. Reads CSV file from data/raw/yolo_detections.csv
2. Loads it into raw.yolo_detections table in PostgreSQL
3. Handles duplicates and data validation
"""

import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv()

# Database connection
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "medical_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_db_connection():
    """Create and return database connection."""
    encoded_password = quote_plus(POSTGRES_PASSWORD)
    connection_string = (
        f"postgresql://{POSTGRES_USER}:{encoded_password}@"
        f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    return create_engine(connection_string)


def create_raw_schema(engine):
    """Create raw schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
        print("✓ Raw schema created/verified")


def create_yolo_detections_table(engine):
    """Create raw.yolo_detections table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw.yolo_detections (
        id SERIAL PRIMARY KEY,
        message_id BIGINT NOT NULL,
        channel_name VARCHAR(255) NOT NULL,
        image_path VARCHAR(500),
        image_category VARCHAR(50),
        num_detections INTEGER,
        max_confidence DECIMAL(5,4),
        detected_classes TEXT,
        detected_class_1 VARCHAR(100),
        confidence_1 DECIMAL(5,4),
        detected_class_2 VARCHAR(100),
        confidence_2 DECIMAL(5,4),
        detected_class_3 VARCHAR(100),
        confidence_3 DECIMAL(5,4),
        detected_class_4 VARCHAR(100),
        confidence_4 DECIMAL(5,4),
        detected_class_5 VARCHAR(100),
        confidence_5 DECIMAL(5,4),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_id, channel_name)
    );
    
    CREATE INDEX IF NOT EXISTS idx_yolo_message_id ON raw.yolo_detections(message_id);
    CREATE INDEX IF NOT EXISTS idx_yolo_channel_name ON raw.yolo_detections(channel_name);
    CREATE INDEX IF NOT EXISTS idx_yolo_category ON raw.yolo_detections(image_category);
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        print("✓ YOLO detections table created/verified")


def load_csv_to_postgres(csv_path: Path, engine):
    """Load CSV file to PostgreSQL."""
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    print(f"Reading CSV file: {csv_path}")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return False
    
    if df.empty:
        print("⚠ CSV file is empty")
        return False
    
    print(f"Found {len(df)} rows in CSV")
    
    # Ensure message_id is integer
    if 'message_id' in df.columns:
        df['message_id'] = pd.to_numeric(df['message_id'], errors='coerce').astype('Int64')
    
    # Ensure num_detections is integer
    if 'num_detections' in df.columns:
        df['num_detections'] = pd.to_numeric(df['num_detections'], errors='coerce').astype('Int64')
    
    # Ensure confidence columns are numeric
    confidence_cols = [col for col in df.columns if col.startswith('confidence_')]
    for col in confidence_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Select columns that match table schema
    table_columns = [
        'message_id', 'channel_name', 'image_path', 'image_category',
        'num_detections', 'max_confidence', 'detected_classes',
        'detected_class_1', 'confidence_1',
        'detected_class_2', 'confidence_2',
        'detected_class_3', 'confidence_3',
        'detected_class_4', 'confidence_4',
        'detected_class_5', 'confidence_5',
    ]
    
    # Only include columns that exist in DataFrame
    available_columns = [col for col in table_columns if col in df.columns]
    df = df[available_columns]
    
    # Load to PostgreSQL
    table_name = "raw.yolo_detections"
    chunk_size = 1000
    total_rows = len(df)
    loaded_rows = 0
    
    print(f"\nLoading {total_rows} detections to {table_name}...")
    
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        
        try:
            # Use ON CONFLICT to handle duplicates
            # First, delete existing rows for these message_id + channel_name combinations
            with engine.connect() as conn:
                for _, row in chunk.iterrows():
                    if pd.notna(row.get('message_id')) and pd.notna(row.get('channel_name')):
                        conn.execute(text("""
                            DELETE FROM raw.yolo_detections
                            WHERE message_id = :msg_id AND channel_name = :channel
                        """), {
                            'msg_id': int(row['message_id']),
                            'channel': str(row['channel_name'])
                        })
                conn.commit()
            
            # Insert new rows
            chunk.to_sql(
                name='yolo_detections',
                schema='raw',
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            loaded_rows += len(chunk)
            print(f"  Loaded {loaded_rows}/{total_rows} detections...", end='\r')
            
        except SQLAlchemyError as e:
            print(f"\n  ⚠ Error loading chunk: {e}")
            continue
    
    print(f"\n✓ Successfully loaded {loaded_rows} detections to {table_name}")
    return True


def get_table_stats(engine):
    """Get statistics about loaded data."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_detections,
                COUNT(DISTINCT message_id) as unique_messages,
                COUNT(DISTINCT channel_name) as unique_channels,
                COUNT(DISTINCT image_category) as unique_categories,
                COUNT(CASE WHEN image_category = 'promotional' THEN 1 END) as promotional_count,
                COUNT(CASE WHEN image_category = 'product_display' THEN 1 END) as product_display_count,
                COUNT(CASE WHEN image_category = 'lifestyle' THEN 1 END) as lifestyle_count,
                COUNT(CASE WHEN image_category = 'other' THEN 1 END) as other_count
            FROM raw.yolo_detections
        """))
        
        stats = result.fetchone()
        if stats:
            print("\n" + "="*50)
            print("YOLO Detections Statistics")
            print("="*50)
            print(f"Total Detections: {stats[0]}")
            print(f"Unique Messages: {stats[1]}")
            print(f"Unique Channels: {stats[2]}")
            print(f"Unique Categories: {stats[3]}")
            print(f"\nCategory Breakdown:")
            print(f"  Promotional: {stats[4]}")
            print(f"  Product Display: {stats[5]}")
            print(f"  Lifestyle: {stats[6]}")
            print(f"  Other: {stats[7]}")
            print("="*50)


def main():
    """Main function to load YOLO detections to PostgreSQL."""
    print("="*50)
    print("Loading YOLO Detections to PostgreSQL")
    print("="*50)
    
    # Get CSV file path
    base_dir = Path(__file__).parent.parent
    csv_path = base_dir / "data" / "raw" / "yolo_detections.csv"
    
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        print("Please run src/yolo_detect.py first to generate the CSV file.")
        sys.exit(1)
    
    # Connect to database
    try:
        engine = get_db_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)
    
    # Create schema and table
    create_raw_schema(engine)
    create_yolo_detections_table(engine)
    
    # Load CSV
    success = load_csv_to_postgres(csv_path, engine)
    
    if success:
        # Get statistics
        get_table_stats(engine)
        print("\n✓ Data loading complete!")
    else:
        print("\n❌ Data loading failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
