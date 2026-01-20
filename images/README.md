# Images Directory

This directory contains visualizations and screenshots for the project documentation.

## Required Images

1. **star_schema_erd.png** - Entity Relationship Diagram of the star schema
   - Shows dim_channels, dim_dates, and fct_messages tables
   - Includes primary keys, foreign keys, and relationships
   - Tools: dbdiagram.io, draw.io, or Lucidchart

2. **dbt_docs_welcome.png** - Screenshot of dbt documentation welcome page
   - Capture from `cd medical_warehouse && dbt docs serve`
   - Shows the welcome page with navigation tabs
   - URL: http://localhost:8080

## How to Capture dbt Docs Screenshot

```bash
cd medical_warehouse
dbt docs generate
dbt docs serve
# Open http://localhost:8080 in browser
# Take screenshot of the welcome page
# Save as images/dbt_docs_welcome.png
```
