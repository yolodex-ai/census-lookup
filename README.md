# census-lookup

A Python library for mapping US addresses to Census 2020 block-level data locally, without relying on rate-limited APIs.

## Features

- **Fully offline geocoding** using TIGER Address Range files (~95% match rate)
- **Lazy per-state data downloading** - only download data for states you need
- **Configurable geographic levels** - block, block group, tract, or county
- **User-selectable Census variables** - choose which PL 94-171 variables to include
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

The library includes PL 94-171 redistricting data:

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

## Data Storage

Data is cached in `~/.census-lookup/`:

```
~/.census-lookup/
├── catalog.json           # Tracks downloaded data
├── tiger/
│   ├── addrfeat/         # Address range features
│   └── blocks/           # Block polygons
└── census/
    └── pl94171/          # Census data
```

Typical storage per state: 100-300MB

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
