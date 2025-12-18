"""Functional tests for single address geocoding.

Tests the core geocoding functionality through the public API.
"""

import pandas as pd

from census_lookup import CensusLookup, GeoLevel


class TestSingleAddressLookup:
    """User can look up a single address and get census data."""

    async def test_basic_lookup(self, mock_census_http, isolated_data_dir):
        """Look up an address, get GEOID and population."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.geoid is not None
        assert result.census_data["P1_001N"] > 0

    async def test_block_vs_county_aggregation(self, mock_census_http, isolated_data_dir):
        """Data is properly aggregated at different geographic levels."""
        block_lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        county_lookup = CensusLookup(
            geo_level=GeoLevel.COUNTY,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        block_result = await block_lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")
        county_result = await county_lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        # GEOIDs have correct length
        assert len(block_result.geoid) == 15
        assert len(county_result.geoid) == 5

        # County population >= block population (aggregation)
        assert county_result.census_data["P1_001N"] >= block_result.census_data["P1_001N"]

    async def test_all_geographic_levels(self, mock_census_http, isolated_data_dir):
        """Test all geographic levels return correct GEOID lengths."""
        address = "1600 Pennsylvania Avenue NW, Washington, DC"

        expected_lengths = {
            GeoLevel.BLOCK: 15,
            GeoLevel.BLOCK_GROUP: 12,
            GeoLevel.TRACT: 11,
            GeoLevel.COUNTY: 5,
            GeoLevel.STATE: 2,
        }

        for level, expected_len in expected_lengths.items():
            lookup = CensusLookup(
                geo_level=level,
                variables=["P1_001N"],
                data_dir=isolated_data_dir,
            )
            result = await lookup.geocode(address)

            assert result.is_matched, f"Level {level} should match"
            msg = f"Level {level} should have {expected_len} digit GEOID"
            assert len(result.geoid) == expected_len, msg

    async def test_geoid_components_populated(self, mock_census_http, isolated_data_dir):
        """GEOID components are correctly parsed from the result."""
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Full block GEOID should be 15 digits
        assert len(result.geoid) == 15
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
            geo_level=GeoLevel.BLOCK,
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
        """Result can be converted to dictionary."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        data = result.to_dict()

        assert "input_address" in data
        assert "geoid" in data
        assert "latitude" in data
        assert "longitude" in data
        assert "P1_001N" in data

    async def test_result_to_series(self, mock_census_http, isolated_data_dir):
        """Result can be converted to pandas Series."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        series = result.to_series()

        assert isinstance(series, pd.Series)
        assert "geoid" in series.index


class TestAddressFormats:
    """Test various address formats through geocoding."""

    async def test_address_with_directionals(self, mock_census_http, isolated_data_dir):
        """Address with directional prefixes parses correctly."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched

    async def test_address_with_cardinal_directional(self, mock_census_http, isolated_data_dir):
        """Address with N/S/E/W directional."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
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
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Tests normalizer ordinal handling
        result = await lookup.geocode("100 1st Street NE, Washington, DC")

        assert result.match_type in ["interpolated", "exact", "no_match"]

    async def test_address_with_abbreviations(self, mock_census_http, isolated_data_dir):
        """Address with street type abbreviations works."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

        assert result.is_matched

    async def test_address_with_zipcode(self, mock_census_http, isolated_data_dir):
        """Address with zipcode parses correctly."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC 20500")

        assert result.is_matched

    async def test_address_lowercase(self, mock_census_http, isolated_data_dir):
        """Lowercase address is normalized and matches."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 pennsylvania avenue nw, washington, dc")

        assert result.is_matched
