#!/bin/bash
# Task 2: Data Modeling and Transformation Pipeline
# This script:
# 1. Loads raw JSON data to PostgreSQL
# 2. Runs dbt transformations to create star schema

set -e

echo "=========================================="
echo "Task 2: Data Modeling and Transformation"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please copy env.template to .env and configure it"
    exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Step 1: Load raw data to PostgreSQL
echo "Step 1: Loading raw data to PostgreSQL..."
echo "----------------------------------------"
python scripts/load_raw_to_postgres.py

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to load raw data"
    exit 1
fi

echo ""
echo "Step 2: Installing dbt packages..."
echo "----------------------------------------"
cd medical_warehouse
dbt deps

echo ""
echo "Step 3: Running dbt transformations..."
echo "----------------------------------------"
# Run staging models first
echo "Building staging models..."
dbt run --select staging

# Then run marts (dimensions and facts)
echo "Building marts (dimensions and facts)..."
dbt run --select marts

echo ""
echo "Step 4: Running dbt tests..."
echo "----------------------------------------"
dbt test

if [ $? -ne 0 ]; then
    echo "⚠️  Warning: Some tests failed. Check the output above."
else
    echo "✅ All tests passed!"
fi

echo ""
echo "Step 5: Generating documentation..."
echo "----------------------------------------"
dbt docs generate

echo ""
echo "=========================================="
echo "✅ Task 2 Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. View dbt documentation: dbt docs serve"
echo "2. Query your star schema tables:"
echo "   - marts.dim_channels"
echo "   - marts.dim_dates"
echo "   - marts.fct_messages"
echo ""

cd ..
