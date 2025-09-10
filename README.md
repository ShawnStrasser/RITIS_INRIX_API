# RITIS INRIX API

[![PyPI](https://img.shields.io/pypi/v/ritis_inrix_api)](https://pypi.org/project/ritis_inrix_api/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ritis_inrix_api)](https://pypi.org/project/ritis_inrix_api/)
[![GitHub License](https://img.shields.io/github/license/ShawnStrasser/RITIS_INRIX_API)](https://github.com/ShawnStrasser/RITIS_INRIX_API/blob/main/LICENSE)

[![GitHub issues](https://img.shields.io/github/issues/ShawnStrasser/RITIS_INRIX_API)](https://github.com/ShawnStrasser/RITIS_INRIX_API/issues)
[![Unit Tests](https://github.com/ShawnStrasser/RITIS_INRIX_API/actions/workflows/pr-tests.yml/badge.svg)](https://github.com/ShawnStrasser/RITIS_INRIX_API/actions/workflows/pr-tests.yml)

A Python package for automated retrieval of traffic data from RITIS and INRIX APIs for INRIX XD Segments.

## Installation

```bash
pip install ritis-inrix-api
```

## Core Functionality

This package provides tools to:
*   **Download Historical Data**: Retrieve historical XD segment data from the RITIS API for specific date ranges or on a daily schedule.
*   **Fetch Real-Time Data**: Get current traffic speeds from the INRIX API.
*   **Scrape Segment Geometries**: Identify and retrieve XD segment geometries based on a list of latitude/longitude coordinates.

## Examples

Here are some examples of how to use the package.

### RITIS API: Download Historical Data

You can download data for a specific date range or set up a recurring daily download.

#### Download data for a single period
```python
import os
from ritis_inrix_api import RITIS_Downloader

segments = [1236893704, 1236860943]

updater = RITIS_Downloader(
    api_key=os.environ.get('RITIS_API_KEY'),
    segments=segments, # can be list of IDs or path to a .txt file
    columns=['speed', 'travel_time_seconds'], # Specify desired columns
    start_time='06:00:00', #default is '00:00:00
    end_time='06:15:00', #default is '23:59:00'
    bin_size=5, #Enter 1, 5, 10, 15(default), or 60
    units='seconds', #'seconds' or 'minutes'
    #download_path='Data', #where to save data

) 

# Returns a pandas DataFrame (unless download_path is specified)
df = updater.single_download('2025-09-01', '2025-09-02', 'test')
```
<details>
<summary>Sample Output</summary>

|    | xd_id      | measurement_tstamp  |   speed |   travel_time_seconds |
|---:|:-----------|:--------------------|--------:|----------------------:|
|  0 | 1236860943 | 2025-09-01 06:00:00 |      25 |                 11.95 |
|  1 | 1236860943 | 2025-09-01 06:05:00 |      25 |                 11.95 |
|  2 | 1236860943 | 2025-09-01 06:10:00 |      25 |                 11.95 |

</details>

#### Set up automated daily downloads
This will download data from the last run date through yesterday one day at a time and save it as Parquet files at the specified download path. This option is intended to run daily via a scheduler like cron or Windows Task Scheduler.
```python
import os
from ritis_inrix_api import RITIS_Downloader

updater = RITIS_Downloader(
    api_key=os.environ.get('RITIS_API_KEY'),
    download_path='Data', # Data will be saved in this directory
    segments='sample_XD_segments.txt', # Path to a file with segment IDs
    last_run_path='last_run.txt' # Path for text file containing last run datetime
) 

updater.daily_download()
```

### INRIX API: Fetch Real-Time Speeds
```python
import os
from ritis_inrix_api import INRIX_Downloader

inrix_downloader = INRIX_Downloader(
    app_id=os.environ.get('INRIX_APP_ID'),
    hash_token=os.environ.get('INRIX_HASH_TOKEN'),
    segments_path='sample_XD_segments.txt'
)

# Returns a pandas DataFrame
speed_data = inrix_downloader.get_speed_data()
```
<details>
<summary>Sample Output</summary>

|    | code       | type   |   speed |   average |
|---:|:-----------|:-------|--------:|----------:|
|  0 | 1236893704 | XDS    |      53 |        52 |
|  1 | 1236860943 | XDS    |      21 |        19 |

</details>

### Geometry Scraper: Find Segments and Geometries
This tool finds XD segment geometries within a specified radius of given latitude/longitude points.

#### How to get the authentication cookie
The Geometry Scraper requires a valid browser cookie from a logged-in RITIS session to authenticate its requests. Open network tab in dev tools look for a request that has a cookie, copy and paste the cookie when prompted in the console.

#### Example Usage
```python
from ritis_inrix_api import GeometryScraper

# A list of (latitude, longitude) tuples
locations = [
    (42.34072155027376, -122.89930147132378),
    (44.3029045246138, -120.842181329508),
    # ... more locations
]

# The scraper will prompt you to paste the cookie into the console
scraper = GeometryScraper()
geometry_data = scraper.process_locations(locations, buffer_size=50) # buffer size in yards

# Save the new segment IDs to a file
segments = list(geometry_data['segID'])
with open('Map_Data/XD_Segments.txt', 'w') as f:
    f.write(','.join(map(str, segments)))
```
<details>
<summary>Sample Output</summary>

The script will print progress messages as it runs:
```
Processing 1002 locations in batches of 500
Processing batch 1
Processing batch 2
Processing batch 3
Combining all batches into a single DataFrame
There were 5008 segments found for the provided signals.
```

The final `geometry_data` DataFrame will look like this:

|    | zip   | country | segID      | bearing | county    | ... | coordinates                                          |
|---:|:------|:--------|:-----------|:--------|:----------|:----|:-----------------------------------------------------|
|  0 | 97051 | USA     | 1237027426 | E       | COLUMBIA  | ... | [[-122.8316, 45.84865], [-122.83134, 45.84852], ...] |
|  1 | 97756 | USA     | 1237004066 | S       | DESCHUTES | ... | [[-121.19391, 44.24292], [-121.1941, 44.2427], ...]   |
|  2 | 97527 | USA     | 125164532  | E       | JOSEPHINE | ... | [[-123.32078, 42.42779], [-123.32054, 42.42772], ...] |
</details>

