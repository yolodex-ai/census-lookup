# census-lookup

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

- **Income**: Median household income, per capita income, poverty status
- **Education**: Educational attainment levels
- **Employment**: Labor force status, occupation, industry
- **Housing**: Home values, rent, tenure, housing characteristics
- **Health Insurance**: Coverage by type
- **Commute**: Transportation to work, travel time
- **And more**: Language, internet access, household composition

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
