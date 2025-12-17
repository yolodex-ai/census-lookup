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
pip install census-lookup
```

## Quick Start

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

### CLI

```bash
# Look up a single address
census-lookup lookup "123 Main St, Los Angeles, CA 90012" --level block

# Process a batch file
census-lookup batch input.csv output.csv --address-column addr --level tract

# Pre-download data for states
census-lookup download CA TX NY

# List available census variables
census-lookup variables

# Show cache info
census-lookup info
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

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run linting
ruff check src/
```

## License

MIT
