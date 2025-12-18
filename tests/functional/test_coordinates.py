"""Functional tests for coordinate-based lookups.

Tests looking up census data by latitude/longitude coordinates.
"""

import pandas as pd
import pytest

from census_lookup import CensusLookup, GeoLevel


class TestCoordinateLookup:
    """User can look up census data by coordinates."""

    async def test_coordinate_lookup(self, mock_census_http, isolated_data_dir):
        """Look up census data for lat/lon coordinates."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        # Load DC by looking up an address first
        await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        # Now look up by coordinates (White House coordinates)
        result = await lookup.lookup_coordinates(38.8977, -77.0365)

        assert result.is_matched
        assert result.geoid is not None
        assert result.census_data.get("P1_001N") is not None

    async def test_coordinate_batch_lookup(self, mock_census_http, isolated_data_dir):
        """Batch coordinate lookup with DataFrame."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        # Load DC first
        await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        # Create DataFrame with coordinates
        df = pd.DataFrame({
            "name": ["White House", "Capitol"],
            "latitude": [38.8977, 38.8899],
            "longitude": [-77.0365, -77.0091],
        })

        results = await lookup.lookup_coordinates_batch(df)

        assert len(results) == 2
        assert "GEOID" in results.columns
