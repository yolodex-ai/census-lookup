"""
census-lookup: Offline address-to-Census 2020 data mapping for Python.

Map US addresses to Census 2020 block-level data locally, avoiding API rate limits.

Supports:
- PL 94-171 (Redistricting Data): Population, race, housing
- ACS 5-Year Estimates: Income, education, employment, housing characteristics
"""

from census_lookup.census.acs import (
    ACS_VARIABLE_GROUPS,
    ACS_VARIABLES,
    get_acs_variables_for_group,
    list_acs_tables,
    list_acs_variable_groups,
)
from census_lookup.census.variables import (
    VARIABLE_GROUPS,
    VARIABLES,
    get_variables_for_group,
    list_tables,
    list_variable_groups,
)
from census_lookup.core.geoid import GeoLevel, GEOIDParser
from census_lookup.core.lookup import CensusLookup, LookupResult

__version__ = "0.1.0"
__all__ = [
    # Main API
    "CensusLookup",
    "LookupResult",
    "GeoLevel",
    "GEOIDParser",
    # PL 94-171 Variables
    "VARIABLES",
    "VARIABLE_GROUPS",
    "get_variables_for_group",
    "list_tables",
    "list_variable_groups",
    # ACS Variables
    "ACS_VARIABLES",
    "ACS_VARIABLE_GROUPS",
    "get_acs_variables_for_group",
    "list_acs_tables",
    "list_acs_variable_groups",
]
