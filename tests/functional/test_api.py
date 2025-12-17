"""Functional tests for the public census-lookup API.

These test the main use cases that users care about:
1. Single address lookup
2. Batch geocoding
3. Different geographic levels
4. ACS data retrieval
5. Coordinate-based lookup

Run with: pytest tests/functional -v -s
"""

from census_lookup import CensusLookup, GeoLevel


class TestSingleAddressLookup:
    """User can look up a single address and get census data."""

    def test_basic_lookup(self):
        """Look up an address, get GEOID and population."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.geoid is not None
        assert result.census_data["P1_001N"] > 0

    def test_block_vs_county_aggregation(self):
        """Data is properly aggregated at different geographic levels."""
        block_lookup = CensusLookup(geo_level=GeoLevel.BLOCK, variables=["P1_001N"])
        county_lookup = CensusLookup(geo_level=GeoLevel.COUNTY, variables=["P1_001N"])

        block_result = block_lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")
        county_result = county_lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        # GEOIDs have correct length
        assert len(block_result.geoid) == 15
        assert len(county_result.geoid) == 5

        # County population > block population
        assert county_result.census_data["P1_001N"] > block_result.census_data["P1_001N"]


class TestBatchLookup:
    """User can geocode multiple addresses at once."""

    def test_batch_geocoding(self):
        """Batch geocode returns DataFrame with all results."""
        lookup = CensusLookup(geo_level=GeoLevel.TRACT, variables=["P1_001N"])

        results = lookup.geocode_batch([
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "100 Maryland Ave SW, Washington, DC",
        ])

        assert len(results) == 2
        assert "geoid" in results.columns
        assert "P1_001N" in results.columns


class TestACSData:
    """User can retrieve ACS data (income, education, etc.)."""

    def test_acs_income_data(self):
        """Get median household income from ACS."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=["B19013_001E"],
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.census_data.get("B19013_001E") is not None

    def test_combined_pl94171_and_acs(self):
        """Get both PL 94-171 (population) and ACS (income) together."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert "P1_001N" in result.census_data
        assert "B19013_001E" in result.census_data


