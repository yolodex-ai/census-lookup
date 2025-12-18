"""Functional tests for CensusLookup configuration.

Tests initialization, state loading, and variable management through the public API.
"""

import pytest

from census_lookup import CensusLookup, GeoLevel


class TestLoadingAndConfiguration:
    """User can configure the lookup instance."""

    async def test_load_state_explicitly(self, mock_census_http, isolated_data_dir):
        """Load state data explicitly before lookups."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        await lookup.load_state("DC")

        assert "11" in lookup.loaded_states

    async def test_load_state_by_full_name(self, mock_census_http, isolated_data_dir):
        """Load state using full state name."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Tests normalize_state with full name
        await lookup.load_state("District of Columbia")

        assert "11" in lookup.loaded_states

    async def test_load_state_by_fips(self, mock_census_http, isolated_data_dir):
        """Load state using FIPS code."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        await lookup.load_state("11")

        assert "11" in lookup.loaded_states

    async def test_load_multiple_states(self, mock_census_http, isolated_data_dir):
        """Load multiple states at once."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        await lookup.load_states(["DC"])

        assert "11" in lookup.loaded_states

    def test_variables_property(self):
        """Access the current variable list."""
        lookup = CensusLookup(
            variables=["P1_001N", "H1_001N"],
        )

        assert "P1_001N" in lookup.variables
        assert "H1_001N" in lookup.variables

    def test_set_variables(self):
        """Change variables after initialization."""
        lookup = CensusLookup(variables=["P1_001N"])

        lookup.set_variables(["P1_001N", "H1_001N"])

        assert "H1_001N" in lookup.variables

    def test_add_variable_group(self):
        """Add a variable group after initialization."""
        lookup = CensusLookup(variables=["P1_001N"])

        lookup.add_variable_group("housing")

        assert "H1_001N" in lookup.variables

    def test_available_variables(self):
        """Get dictionary of all available variables."""
        lookup = CensusLookup()

        available = lookup.available_variables

        assert "P1_001N" in available
        assert isinstance(available["P1_001N"], str)

    def test_acs_variables_property(self):
        """Access ACS variable list."""
        lookup = CensusLookup(
            acs_variables=["B19013_001E"],
        )

        assert "B19013_001E" in lookup.acs_variables

    def test_set_acs_variables(self):
        """Change ACS variables after initialization."""
        lookup = CensusLookup()

        lookup.set_acs_variables(["B19013_001E"])

        assert "B19013_001E" in lookup.acs_variables

    def test_add_acs_variable_group(self):
        """Add ACS variable group after initialization."""
        lookup = CensusLookup()

        lookup.add_acs_variable_group("income")

        assert "B19013_001E" in lookup.acs_variables

    def test_clear_acs_variables(self):
        """Clear all ACS variables."""
        lookup = CensusLookup(acs_variables=["B19013_001E"])

        lookup.clear_acs_variables()

        assert len(lookup.acs_variables) == 0

    def test_available_acs_variables(self):
        """Get dictionary of available ACS variables."""
        lookup = CensusLookup()

        available = lookup.available_acs_variables

        assert "B19013_001E" in available

    def test_available_acs_variable_groups(self):
        """Get dictionary of ACS variable groups."""
        lookup = CensusLookup()

        groups = lookup.available_acs_variable_groups

        assert "income" in groups


class TestDataDownload:
    """Test that data downloading works correctly."""

    async def test_download_triggers_on_first_lookup(self, mock_census_http, isolated_data_dir):
        """Data is downloaded automatically when needed."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
            auto_download=True,
        )

        # This should trigger download via mocked endpoints
        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert "11" in lookup.loaded_states

    async def test_default_variables(self, mock_census_http, isolated_data_dir):
        """No variables specified uses default (P1_001N)."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Default should include at least population
        assert "P1_001N" in result.census_data
