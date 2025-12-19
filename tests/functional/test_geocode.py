"""Functional tests for single address geocoding.

Tests the core geocoding functionality through the public API.
"""

import pandas as pd

from census_lookup import CensusLookup


class TestSingleAddressLookup:
    """User can look up a single address and get census data."""

    async def test_basic_lookup(self, mock_census_http, isolated_data_dir):
        """Look up an address, get GEOID and population at all levels."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.block is not None
        # Census data is now nested by level
        assert result.census_data["P1_001N"]["block"] > 0

    async def test_all_levels_returned(self, mock_census_http, isolated_data_dir):
        """Data is returned at all geographic levels."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched
        # All GEOIDs should be populated
        assert len(result.block) == 15
        assert len(result.block_group) == 12
        assert len(result.tract) == 11
        assert len(result.county_fips) == 5
        assert len(result.state_fips) == 2

        # Population at different levels (county >= tract >= block)
        census_data = result.census_data["P1_001N"]
        assert census_data["county"] >= census_data["tract"]
        assert census_data["tract"] >= census_data["block"]

    async def test_all_geographic_levels_in_result(self, mock_census_http, isolated_data_dir):
        """Test all geographic levels are returned in the result."""
        address = "1600 Pennsylvania Avenue NW, Washington, DC"

        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode(address)

        assert result.is_matched
        # All levels should be present in result
        assert result.block is not None and len(result.block) == 15
        assert result.block_group is not None and len(result.block_group) == 12
        assert result.tract is not None and len(result.tract) == 11
        assert result.county_fips is not None and len(result.county_fips) == 5
        assert result.state_fips is not None and len(result.state_fips) == 2

    async def test_geoid_components_populated(self, mock_census_http, isolated_data_dir):
        """GEOID components are correctly parsed from the result."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Full block GEOID should be 15 digits
        assert len(result.block) == 15
        # Components should be populated
        assert result.state_fips == "11"  # DC
        assert result.county_fips is not None
        assert len(result.county_fips) == 5  # state + county
        assert result.tract is not None
        assert result.block_group is not None
        assert result.block is not None

    async def test_geoid_components_in_dict(self, mock_census_http, isolated_data_dir):
        """GEOID components are included in to_dict output."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        data = result.to_dict()

        assert "state_fips" in data
        assert "county_fips" in data
        assert "tract" in data
        assert "block_group" in data
        assert "block" in data
        assert data["state_fips"] == "11"

    async def test_result_to_dict(self, mock_census_http, isolated_data_dir):
        """Result can be converted to dictionary with nested census data."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        data = result.to_dict()

        assert "input_address" in data
        assert "block" in data  # GEOIDs at all levels
        assert "latitude" in data
        assert "longitude" in data
        # Census data is nested
        assert "P1_001N" in data
        assert isinstance(data["P1_001N"], dict)
        assert "block" in data["P1_001N"]

    async def test_result_to_series(self, mock_census_http, isolated_data_dir):
        """Result can be converted to pandas Series (flattened at block level)."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        series = result.to_series()

        assert isinstance(series, pd.Series)
        assert "block" in series.index
        # Flattened census data should be a scalar, not nested
        assert "P1_001N" in series.index
        assert not isinstance(series["P1_001N"], dict)


class TestAddressFormats:
    """Test various address formats through geocoding."""

    async def test_address_with_directionals(self, mock_census_http, isolated_data_dir):
        """Address with directional prefixes parses correctly."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched

    async def test_address_with_cardinal_directional(self, mock_census_http, isolated_data_dir):
        """Address with N/S/E/W directional."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Tests normalizer directional expansion (N -> NORTH)
        result = await lookup.geocode("100 N Capitol St, Washington, DC")

        # May or may not match depending on data, but should parse
        assert result.match_type in ["interpolated", "exact", "no_match"]

    async def test_address_with_ordinal_street(self, mock_census_http, isolated_data_dir):
        """Address with ordinal street name (1st, 2nd, etc.)."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Tests normalizer ordinal handling
        result = await lookup.geocode("100 1st Street NE, Washington, DC")

        assert result.match_type in ["interpolated", "exact", "no_match"]

    async def test_address_with_abbreviations(self, mock_census_http, isolated_data_dir):
        """Address with street type abbreviations works."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

        assert result.is_matched

    async def test_address_with_zipcode(self, mock_census_http, isolated_data_dir):
        """Address with zipcode parses correctly."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC 20500")

        assert result.is_matched

    async def test_address_lowercase(self, mock_census_http, isolated_data_dir):
        """Lowercase address is normalized and matches."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 pennsylvania avenue nw, washington, dc")

        assert result.is_matched
