"""Functional tests for coordinate-based lookups.

Tests looking up census data by latitude/longitude coordinates.
"""

import pandas as pd

from census_lookup import CensusLookup


class TestCoordinateLookup:
    """User can look up census data by coordinates."""

    async def test_coordinate_lookup(self, mock_census_http, isolated_data_dir):
        """Look up census data for lat/lon coordinates."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        # Load DC by looking up an address first
        await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        # Now look up by coordinates (White House coordinates)
        result = await lookup.lookup_coordinates(38.8977, -77.0365)

        assert result.is_matched
        assert result.block is not None
        # Census data is nested by level
        assert "P1_001N" in result.census_data
        assert result.census_data["P1_001N"].get("block") is not None

    async def test_coordinate_batch_lookup(self, mock_census_http, isolated_data_dir):
        """Batch coordinate lookup with DataFrame."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        # Load DC first
        await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        # Create DataFrame with coordinates
        df = pd.DataFrame(
            {
                "name": ["White House", "Capitol"],
                "latitude": [38.8977, 38.8899],
                "longitude": [-77.0365, -77.0091],
            }
        )

        results = await lookup.lookup_coordinates_batch(df)

        assert len(results) == 2
        assert "GEOID" in results.columns

    async def test_coordinate_lookup_with_acs_null_values(
        self, mock_census_http_acs_with_nulls, isolated_data_dir_acs_nulls
    ):
        """Coordinate lookup handles ACS null values correctly."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],  # Median income - will be null
            data_dir=isolated_data_dir_acs_nulls,
        )
        # Load DC first
        await lookup.load_state("DC")

        # Look up by coordinates
        result = await lookup.lookup_coordinates(38.8977, -77.0365)

        assert result.is_matched
        # The ACS variable should have tract level with None value
        acs_data = result.census_data.get("B19013_001E")
        assert acs_data is None or acs_data.get("tract") is None
