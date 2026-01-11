# Stop-and-Search Data Collection Guide

## Overview

The `data_collection_script.py` downloads stop-and-search data from the UK Police API (police.uk) across multiple police forces and time periods using the api: https://data.police.uk/api. For more information about the api we recommend to visit the full documentation at: https://data.police.uk/docs/.

## How It Works

1. **Fetch Available Dates** - Queries the API for all available months with data
2. **Filter by Date Range** - Optionally filters to specified date range
3. **Iterate Through Forces** - For each month, fetches records from all police forces
4. **Save Data** - Combines all records and saves to `data/stop_search_data/`

## Quick Start

### Download All Available Data

```bash
python scripts/data_collection_script.py
```

### Download Data from 2022 to 2025

```bash
python scripts/data_collection_script.py --start 2022-01 --end 2025-12
```

## Command-Line Options

```bash
--format [json|csv|parquet|all]  # Output format (default: parquet)
--start YYYY-MM                   # Start date (optional)
--end YYYY-MM                     # End date (optional)
```

## More Examples

```bash
# Download 2022-2025 data in CSV format
python scripts/data_collection_script.py --format csv --start 2022-01 --end 2025-12

# Download all formats
python scripts/data_collection_script.py --format all --start 2022-01 --end 2025-12

# Download only 2024 data
python scripts/data_collection_script.py --start 2024-01 --end 2024-12
```

## Output

Files are saved to:  `data/stop_search_data/ssd_{start_date}-{end_date}.{format}`

**Example:** `data/stop_search_data/ssd_2022-01-2025-12.parquet`

## Expected Performance

- **Time**: 15-30 minutes for 2022-2025 full download
- **File Size**: ~50-100 MB (Parquet), ~200-400 MB (CSV)
- **Records**: 500,000+ records for 4 years of data

## Next Steps

Load and explore your data: 

```python
import pandas as pd
df = pd.read_parquet('data/stop_search_data/ssd_2022-01-2025-12.parquet')
print(df.shape)
print(df.info())
```
