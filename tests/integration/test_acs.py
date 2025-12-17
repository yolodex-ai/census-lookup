"""Integration tests for ACS (American Community Survey) data.

These tests verify that ACS data can be retrieved and joined correctly.
ACS data is only available at tract level and above.

Run with: pytest tests/integration/test_acs.py -v -s
"""

import pytest

from census_lookup import CensusLookup, GeoLevel, list_acs_variable_groups


class TestACSVariableGroups:
    """Test ACS variable group functionality."""

    def test_list_acs_groups(self):
        """Verify we can list ACS variable groups."""
        groups = list_acs_variable_groups()

        assert isinstance(groups, dict)
        assert len(groups) > 0

        # Should have common groups
        assert "income" in groups
        assert "education" in groups
        assert "housing" in groups

    def test_income_group_has_variables(self):
        """Verify income group contains expected variables."""
        from census_lookup import get_acs_variables_for_group

        income_vars = get_acs_variables_for_group("income")

        assert len(income_vars) > 0
        # Should include median household income
        assert "B19013_001E" in income_vars


class TestACSDataRetrieval:
    """Test retrieving ACS data with geocoding."""

    @pytest.fixture(scope="class")
    def lookup_with_acs(self):
        """Create lookup with both PL 94-171 and ACS variables."""
        return CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],  # PL 94-171 population
            acs_variables=["B19013_001E", "B25077_001E"],  # Income, home value
            auto_download=True,
        )

    def test_acs_income_data(self, lookup_with_acs):
        """Test retrieving median household income."""
        result = lookup_with_acs.geocode(
            "1600 Pennsylvania Avenue NW, Washington, DC"
        )

        assert result.is_matched

        # Should have both PL 94-171 and ACS data
        assert "P1_001N" in result.census_data  # Population
        assert "B19013_001E" in result.census_data  # Median income

        income = result.census_data.get("B19013_001E")
        # Income should be a positive number (or None if not available)
        if income is not None:
            assert income > 0

    def test_acs_with_variable_groups(self):
        """Test using ACS variable groups instead of individual variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variable_groups=["income"],
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Should have income-related variables
        assert any(
            k.startswith("B19") for k in result.census_data.keys()
        ), "Should have income variables (B19xxx)"


class TestACSGeographicLevels:
    """Test ACS data at different geographic levels."""

    def test_acs_at_tract_level(self):
        """ACS should work at tract level."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=["B19013_001E"],
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert len(result.geoid) == 11  # Tract GEOID
        assert "B19013_001E" in result.census_data

    def test_acs_at_county_level(self):
        """ACS should work at county level with aggregation."""
        lookup = CensusLookup(
            geo_level=GeoLevel.COUNTY,
            acs_variables=["B19013_001E"],
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.geoid == "11001"  # DC county

        # County-level median income
        income = result.census_data.get("B19013_001E")
        if income is not None:
            assert income > 0

    def test_acs_with_block_level_geocoding(self):
        """Test ACS data when geocoding at block level.

        ACS data is at tract level, so it should still be joined
        even when geocoding at block level.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],  # Block-level data
            acs_variables=["B19013_001E"],  # Tract-level ACS
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert len(result.geoid) == 15  # Block-level GEOID

        # Should still have ACS data (joined at tract level)
        assert "B19013_001E" in result.census_data


class TestACSBatchProcessing:
    """Test ACS data in batch operations."""

    def test_batch_with_acs(self):
        """Test batch geocoding with ACS variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],
            auto_download=True,
        )

        addresses = [
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "100 Maryland Ave SW, Washington, DC",
        ]

        results = lookup.geocode_batch(addresses)

        assert len(results) == 2
        assert "P1_001N" in results.columns
        assert "B19013_001E" in results.columns

        # At least one should have income data
        has_income = results["B19013_001E"].notna().sum()
        assert has_income >= 1


class TestACSDataQuality:
    """Test that ACS data values are reasonable."""

    def test_dc_median_income_reasonable(self):
        """DC median income should be in a reasonable range."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=["B19013_001E"],  # Median household income
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        income = result.census_data.get("B19013_001E")

        # DC is a high-income area
        # Median income should be between $30k and $300k for most tracts
        if income is not None:
            assert 30000 < income < 300000, f"Income {income} seems unreasonable"

    def test_multiple_acs_variables(self):
        """Test retrieving multiple ACS variables at once."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=[
                "B19013_001E",  # Median household income
                "B19301_001E",  # Per capita income
                "B25077_001E",  # Median home value
            ],
            auto_download=True,
        )

        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched

        # Should have all three variables
        assert "B19013_001E" in result.census_data
        assert "B19301_001E" in result.census_data
        assert "B25077_001E" in result.census_data

        # Per capita should be less than median household
        median = result.census_data.get("B19013_001E")
        per_capita = result.census_data.get("B19301_001E")

        if median and per_capita:
            # Per capita is typically less than median household
            # (household = multiple people)
            assert per_capita < median * 2
