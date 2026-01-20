"""
Dagster Pipeline for Medical Telegram Data Warehouse

This pipeline orchestrates the entire data workflow:
1. Scrape Telegram channels
2. Load raw data to PostgreSQL
3. Run YOLO object detection enrichment
4. Transform data with dbt
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from dagster import (
    Definitions,
    OpExecutionContext,
    ScheduleDefinition,
    job,
    op,
)

# Get the base directory
BASE_DIR = Path(__file__).parent


@op(
    description="Scrape messages and images from Telegram channels",
    tags={"component": "scraper", "stage": "extract"},
)
def scrape_telegram_data(context: OpExecutionContext) -> dict:
    """Run the Telegram scraper to collect messages and images."""
    context.log.info("Starting Telegram data scraping...")
    
    scraper_script = BASE_DIR / "src" / "scraper.py"
    
    if not scraper_script.exists():
        raise FileNotFoundError(f"Scraper script not found: {scraper_script}")
    
    try:
        # Run the scraper script
        result = subprocess.run(
            [sys.executable, str(scraper_script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        
        context.log.info("Telegram scraping completed successfully")
        context.log.debug(f"Scraper output: {result.stdout}")
        
        return {
            "status": "success",
            "message": "Telegram data scraped successfully",
        }
    
    except subprocess.CalledProcessError as e:
        context.log.error(f"Scraper failed: {e.stderr}")
        raise


@op(
    description="Load raw JSON data from data lake to PostgreSQL",
    tags={"component": "loader", "stage": "load"},
)
def load_raw_to_postgres(context: OpExecutionContext, scrape_result: dict) -> dict:
    """Load raw JSON files to PostgreSQL database."""
    context.log.info("Loading raw data to PostgreSQL...")
    
    loader_script = BASE_DIR / "scripts" / "load_raw_to_postgres.py"
    
    if not loader_script.exists():
        raise FileNotFoundError(f"Loader script not found: {loader_script}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(loader_script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        
        context.log.info("Data loading completed successfully")
        context.log.debug(f"Loader output: {result.stdout}")
        
        return {
            "status": "success",
            "message": "Raw data loaded to PostgreSQL successfully",
        }
    
    except subprocess.CalledProcessError as e:
        context.log.error(f"Data loading failed: {e.stderr}")
        raise


@op(
    description="Run YOLO object detection on images and load results to database",
    tags={"component": "yolo", "stage": "enrich"},
)
def run_yolo_enrichment(context: OpExecutionContext, scrape_result: dict) -> dict:
    """Run YOLO object detection and load detections to PostgreSQL."""
    context.log.info("Starting YOLO object detection...")
    
    # Step 1: Run YOLO detection
    yolo_script = BASE_DIR / "src" / "yolo_detect.py"
    
    if not yolo_script.exists():
        raise FileNotFoundError(f"YOLO script not found: {yolo_script}")
    
    try:
        context.log.info("Running YOLO detection on images...")
        result = subprocess.run(
            [sys.executable, str(yolo_script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        
        context.log.info("YOLO detection completed")
        context.log.debug(f"YOLO output: {result.stdout}")
        
        # Step 2: Load YOLO detections to database
        loader_script = BASE_DIR / "scripts" / "load_yolo_detections.py"
        
        if not loader_script.exists():
            raise FileNotFoundError(f"YOLO loader script not found: {loader_script}")
        
        context.log.info("Loading YOLO detections to PostgreSQL...")
        result = subprocess.run(
            [sys.executable, str(loader_script)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        
        context.log.info("YOLO enrichment completed successfully")
        context.log.debug(f"YOLO loader output: {result.stdout}")
        
        return {
            "status": "success",
            "message": "YOLO enrichment completed successfully",
        }
    
    except subprocess.CalledProcessError as e:
        context.log.error(f"YOLO enrichment failed: {e.stderr}")
        raise


@op(
    description="Run dbt transformations to create star schema",
    tags={"component": "dbt", "stage": "transform"},
)
def run_dbt_transformations(
    context: OpExecutionContext,
    load_result: dict,
    yolo_result: dict,
) -> dict:
    """Execute dbt models to transform raw data into star schema."""
    context.log.info("Starting dbt transformations...")
    
    dbt_project_dir = BASE_DIR / "medical_warehouse"
    
    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"dbt project directory not found: {dbt_project_dir}")
    
    try:
        # Install dbt dependencies
        context.log.info("Installing dbt packages...")
        subprocess.run(
            ["dbt", "deps"],
            cwd=str(dbt_project_dir),
            check=True,
            capture_output=True,
        )
        
        # Run staging models first
        context.log.info("Building staging models...")
        subprocess.run(
            ["dbt", "run", "--select", "staging"],
            cwd=str(dbt_project_dir),
            check=True,
            capture_output=True,
        )
        
        # Then run marts (dimensions and facts)
        context.log.info("Building marts (dimensions and facts)...")
        result = subprocess.run(
            ["dbt", "run", "--select", "marts"],
            cwd=str(dbt_project_dir),
            check=True,
            capture_output=True,
            text=True,
        )
        
        # Run tests
        context.log.info("Running dbt tests...")
        test_result = subprocess.run(
            ["dbt", "test"],
            cwd=str(dbt_project_dir),
            capture_output=True,
            text=True,
        )
        
        if test_result.returncode != 0:
            context.log.warning("Some dbt tests failed. Check output for details.")
            context.log.debug(f"Test output: {test_result.stdout}")
        else:
            context.log.info("All dbt tests passed!")
        
        context.log.info("dbt transformations completed successfully")
        
        return {
            "status": "success",
            "message": "dbt transformations completed successfully",
        }
    
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt transformation failed: {e.stderr}")
        raise


@job(
    description="Complete data pipeline: scrape, load, enrich, and transform",
    tags={"pipeline": "medical_telegram_warehouse"},
)
def medical_telegram_pipeline() -> None:
    """
    Main pipeline job that orchestrates all data operations.
    
    Execution order:
    1. Scrape Telegram data (parallel start)
    2. Load raw data to PostgreSQL (depends on scrape)
    3. Run YOLO enrichment (depends on scrape)
    4. Run dbt transformations (depends on both load and YOLO)
    """
    # Step 1: Scrape Telegram data
    scrape_result = scrape_telegram_data()
    
    # Step 2 & 3: Load data and run YOLO in parallel (both depend on scrape)
    load_result = load_raw_to_postgres(scrape_result)
    yolo_result = run_yolo_enrichment(scrape_result)
    
    # Step 4: Run dbt transformations (depends on both load and YOLO)
    run_dbt_transformations(load_result, yolo_result)


# Schedule: Run daily at 2 AM UTC
daily_schedule = ScheduleDefinition(
    job=medical_telegram_pipeline,
    name="daily_pipeline_schedule",
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    description="Run the medical telegram data pipeline daily at 2 AM UTC",
)


# Sensor for failure alerts (optional - can be extended with actual alerting)
# This sensor can be enabled to monitor pipeline runs and trigger alerts on failures
# To implement: add logic to check recent run status and send notifications


# Define all assets for Dagster
defs = Definitions(
    jobs=[medical_telegram_pipeline],
    schedules=[daily_schedule],
)
