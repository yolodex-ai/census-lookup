# census-lookup

[![CI](https://github.com/yolodex-ai/census-lookup/actions/workflows/ci.yml/badge.svg)](https://github.com/yolodex-ai/census-lookup/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/census-lookup.svg)](https://pypi.org/project/census-lookup/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/yolodex-ai/census-lookup)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python library for mapping US addresses to Census 2020 block-level data locally, without relying on rate-limited APIs.

## Features

- **Fully offline geocoding** using TIGER Address Range files (~95% match rate)
- **Lazy per-state data downloading** - only download data for states you need
- **Configurable geographic levels** - block, block group, tract, or county
- **Two Census data sources**:
  - **PL 94-171** (Redistricting Data): Population, race, housing counts at block level
  - **ACS 5-Year Estimates**: Income, education, employment, housing characteristics at tract level
- **Efficient batch processing** using DuckDB for fast joins
- **CLI and Python API** - use from command line or in your code

## Installation

```bash
# Using uv (recommended)
uv add census-lookup

# Using pip
pip install census-lookup
```

## Quick Start

### CLI (no install required)

```bash
# Look up a single address (auto-downloads data as needed)
uvx census-lookup lookup "123 Main St, Los Angeles, CA 90012" --level block

# Include specific census variables
uvx census-lookup lookup "123 Main St, Los Angeles, CA 90012" -v P1_001N -v H1_001N

# Process a batch file
uvx census-lookup batch input.csv output.csv --address-column addr --level tract

# Pre-download data for states (optional - data downloads automatically)
uvx census-lookup download CA TX NY

# List available census variables
uvx census-lookup variables

# Show cache info
uvx census-lookup info
```

### Example Output

```json
{
  "input_address": "1600 Pennsylvania Avenue NW, Washington, DC 20500",
  "matched_address": "Pennsylvania Ave NW",
  "latitude": 38.898761,
  "longitude": -77.035117,
  "match_type": "interpolated",
  "match_score": 0.9,
  "geoid": "110010101003014",
  "state_fips": "11",
  "county_fips": "11001",
  "tract": "11001010100",
  "block_group": "110010101003",
  "block": "110010101003014",
  "P1_001N": 19.0,
  "B19013_001E": 72500.0,
  "B25077_001E": 485000.0
}
```

### Python API

```python
from census_lookup import CensusLookup, GeoLevel

# Initialize (first use will download data for the state)
lookup = CensusLookup(
    geo_level=GeoLevel.BLOCK,
    variables=["P1_001N", "H1_001N"],  # Population, Housing units
)

# Single address lookup
result = lookup.geocode("123 Main St, Los Angeles, CA 90012")
print(f"GEOID: {result.geoid}")
print(f"Population: {result.census_data['P1_001N']}")

# Batch processing
import pandas as pd
df = pd.read_csv("addresses.csv")
results = lookup.geocode_batch(df["address"], progress=True)
```

## Geographic Levels

| Level | GEOID Length | Example |
|-------|--------------|---------|
| State | 2 | `06` |
| County | 5 | `06037` |
| Tract | 11 | `06037210100` |
| Block Group | 12 | `060372101001` |
| Block | 15 | `060372101001023` |

## Census Variables

### PL 94-171 (Redistricting Data)

Available at **block level** and above. Includes:

- **P1**: Race (total population, by race categories)
- **P2**: Hispanic/Latino by Race
- **P3**: Race for Population 18+ (voting age)
- **P4**: Hispanic/Latino 18+
- **H1**: Housing Units (total, occupied, vacant)

```python
# Use variable groups
lookup = CensusLookup(variable_groups=["population", "housing"])

# Or specify individual variables
lookup = CensusLookup(variables=["P1_001N", "P1_003N", "H1_001N"])
```

### ACS 5-Year Estimates (American Community Survey)

Available at **tract level** and above. Includes richer demographic data:

| Category | Key Variables | Description |
|----------|---------------|-------------|
| **Income** | `B19013_001E`, `B19301_001E` | Median household income, per capita income |
| **Poverty** | `B17001_001E`, `B17001_002E` | Total population, below poverty level |
| **Education** | `B15003_022E`, `B15003_023E` | Bachelor's degree, Master's degree |
| **Employment** | `B23025_004E`, `B23025_005E` | Employed, Unemployed |
| **Housing** | `B25077_001E`, `B25064_001E` | Median home value, median rent |
| **Tenure** | `B25003_002E`, `B25003_003E` | Owner-occupied, Renter-occupied |
| **Health** | `B27010_017E`, `B27010_050E` | Employer insurance, Medicare |
| **Commute** | `B08301_003E`, `B08301_010E` | Drove alone, Public transit |
| **Internet** | `B28002_004E`, `B28002_013E` | Broadband access, No internet |
| **Language** | `B16001_002E`, `B16001_003E` | English only, Spanish |

Over 100+ ACS variables available. Run `uvx census-lookup variables --acs` for the full list

```python
from census_lookup import CensusLookup, GeoLevel, list_acs_variable_groups

# See available ACS variable groups
print(list_acs_variable_groups())

# Use ACS variables with your lookup
lookup = CensusLookup(
    geo_level=GeoLevel.TRACT,
    variables=["P1_001N"],  # PL 94-171 population
    acs_variables=["B19013_001E", "B25077_001E"],  # Median income, home value
    # Or use variable groups:
    # acs_variable_groups=["income", "housing"],
)

result = lookup.geocode("123 Main St, Los Angeles, CA 90012")
print(f"Median Income: ${result.census_data['B19013_001E']:,}")
```

**Note**: ACS data is available at tract level and above. When using block-level
geocoding with ACS variables, the ACS data is joined at tract level.

## Data Storage

Data is cached in `~/.census-lookup/`:

```
~/.census-lookup/
├── catalog.json           # Tracks downloaded data
├── tiger/
│   ├── addrfeat/         # Address range features
│   └── blocks/           # Block polygons
└── census/
    ├── pl94171/          # PL 94-171 data
    └── acs5/             # ACS 5-Year data
        └── tract/        # ACS at tract level
```

Typical storage per state: 100-300MB (TIGER + PL 94-171), plus ~10-50MB for ACS

## How It Works

1. **Parse address** using the `usaddress` library
2. **Normalize street name** for TIGER matching
3. **Match to TIGER Address Range** segment
4. **Interpolate coordinates** along the street segment
5. **Spatial lookup** using rtree index to find containing census block
6. **Join census data** using DuckDB for efficient queries

## Data Sources

All data is downloaded from official US Census Bureau sources:

- **TIGER/Line Shapefiles**: Geographic boundaries and address ranges
  - https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
  - Address Range Feature files (ADDRFEAT) for geocoding
  - Block shapefiles for spatial lookups

- **PL 94-171 Redistricting Data**: Population and housing counts
  - https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html
  - Available at block level and above

- **American Community Survey (ACS) 5-Year Estimates**: Socioeconomic data
  - https://www.census.gov/programs-surveys/acs
  - Available at tract level and above
  - Accessed via Census API: https://api.census.gov

## Development

```bash
# Clone and install with uv
git clone https://github.com/yolodex-ai/census-lookup.git
cd census-lookup
uv sync --all-extras

# Run unit tests (fast, no network required)
uv run pytest tests/unit -v

# Run functional tests (downloads real data, slower)
uv run pytest tests/functional -v -s

# Run all tests
uv run pytest tests/ -v

# Run linting
uv run ruff check src/
```

## License

MIT
