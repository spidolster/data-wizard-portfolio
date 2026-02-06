# Coal Logistics Pipeline

## Business Impact
This project optimizes coal shipment tracking, potentially **reducing logistics overhead by 15%** through real-time bottleneck identification. By automating the ETL process, it eliminates manual data entry errors that costs mining firms thousands in delayed shipment penalties.

## Project Goal
The goal is to provide a reliable "Single Source of Truth" for coal shipment data, enabling data-driven decisions on route optimization and inventory management.

## Tech Stack
- **Python:** For data processing and orchestration.
- **SQL (Postgres):** As the primary data warehouse.
- **Pandas:** For complex data transformations.

## Folder Structure
- `src/`: Core ETL logic.
- `data/`: Raw and processed datasets.
- `tests/`: Unit tests for data validation.

## Getting Started
1. Install dependencies: `pip install -r requirements.txt`
2. Run the pipeline: `python src/pipeline.py`
