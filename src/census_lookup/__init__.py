"""
census-lookup: Offline address-to-Census 2020 data mapping for Python.

Map US addresses to Census 2020 block-level data locally, avoiding API rate limits.
"""

from census_lookup.core.geoid import GeoLevel, GEOIDParser
from census_lookup.core.lookup import CensusLookup, LookupResult

__version__ = "0.1.0"
__all__ = [
    "CensusLookup",
    "LookupResult",
    "GeoLevel",
    "GEOIDParser",
]
