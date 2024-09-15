# SLC Health Inspections

Scraper script for the [Salt Lake County Health Department's inspections site](https://www.saltlakecounty.gov/health/inspection/). The script iterates over the table to generate detailed output of establishments and their inspection histories.

## Usage

This project uses poetry for its dependencies. Tweak the `scrape.py` file as necessary. By default, the script scrapes all Food Service establishment records and details.

```
poetry install
poetry run python -m slc_health_inspections.scrape
```