# RITIS_INRIX_API

A Python toolkit for automated retrieval of traffic data from RITIS and INRIX APIs, supporting both historical and real-time data collection.

## Overview

This repository provides tools to:
- Download historical XD segment data from the RITIS API on daily or hourly schedules
- Fetch real-time speed data from the INRIX API
- Scrape segment geometries to maintain up-to-date segment lists

## Production Use

This toolkit has been used in production by Oregon DOT since October 2024 to download travel time data from the RITIS API for XD segments at traffic signals on a daily basis.

## Features

- **RITIS API Integration**: Download historical traffic data with configurable parameters
  - Supports daily or hourly scheduled downloads
  - Configurable time bins (5, 10, 15, or 60 minutes)
  - Customizable data columns and confidence scores
  - Automatic job submission, monitoring, and data processing

- **INRIX API Integration**: Access real-time traffic data
  - Token management with automatic refresh
  - Batch processing for large segment lists
  - Real-time speed data retrieval

- **Geometry Scraper**: Maintain up-to-date segment lists
  - Find segments near specified coordinates
  - Extract detailed segment geometry information
  - Process locations in batches for efficiency

## Usage

Please see the [Examples.ipynb](Examples.ipynb) notebook for detailed usage examples, including:
- Setting up daily data downloads from RITIS
- Performing one-time historical data downloads
- Fetching real-time speed data from INRIX
- Updating segment lists and geometries

## Requirements

- Python 3.6+
- Required packages: requests, pandas, duckdb, uuid, zipfile, json

## Configuration

Both APIs require authentication:
- RITIS API requires an API key
- INRIX API requires an app ID and hash token

Store these credentials securely and reference them in your code.

## Data Storage

Data is stored in Parquet format for efficient storage and fast query performance.