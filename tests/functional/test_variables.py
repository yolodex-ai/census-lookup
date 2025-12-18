"""Functional tests for census variables.

Tests PL 94-171 variables, ACS variables, and variable groups through the public API.
"""

import pytest

from census_lookup import (
    ACS_VARIABLE_GROUPS,
    ACS_VARIABLES,
    VARIABLE_GROUPS,
    VARIABLES,
    CensusLookup,
    GeoLevel,
    get_acs_variables_for_group,
    get_variables_for_group,
    list_acs_tables,
    list_acs_variable_groups,
    list_tables,
    list_variable_groups,
)


class TestACSData:
    """User can retrieve ACS data (income, education, etc.)."""

    async def test_acs_income_data(self, mock_census_http, isolated_data_dir):
        """Get median household income from ACS."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=["B19013_001E"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.census_data.get("B19013_001E") is not None

        # Properly close to clean up ACS session
        await lookup.close()

    async def test_combined_pl94171_and_acs(self, mock_census_http, isolated_data_dir):
        """Get both PL 94-171 (population) and ACS (income) together."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert "P1_001N" in result.census_data
        assert "B19013_001E" in result.census_data

    async def test_acs_variable_groups(self, mock_census_http, isolated_data_dir):
        """Use ACS variable groups instead of individual variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variable_groups=["income"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Income group should include median household income
        assert result.census_data.get("B19013_001E") is not None


class TestVariableGroups:
    """User can use variable groups instead of individual codes."""

    async def test_population_group(self, mock_census_http, isolated_data_dir):
        """Use 'population' group to get all population variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variable_groups=["population"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert "P1_001N" in result.census_data

    async def test_housing_group(self, mock_census_http, isolated_data_dir):
        """Use 'housing' group to get housing variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variable_groups=["housing"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert "H1_001N" in result.census_data

    async def test_combined_groups_and_variables(self, mock_census_http, isolated_data_dir):
        """Combine variable groups with individual variables."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_003N"],
            variable_groups=["housing"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Individual variable
        assert "P1_003N" in result.census_data
        # From group
        assert "H1_001N" in result.census_data


class TestVariableDictionaries:
    """Test the exported variable dictionaries and functions."""

    def test_variables_dict_has_population(self):
        """VARIABLES dict contains population variable."""
        assert "P1_001N" in VARIABLES
        assert isinstance(VARIABLES["P1_001N"], str)

    def test_variable_groups_has_population(self):
        """VARIABLE_GROUPS has population group."""
        assert "population" in VARIABLE_GROUPS
        assert "P1_001N" in VARIABLE_GROUPS["population"]

    def test_acs_variables_has_income(self):
        """ACS_VARIABLES has median income."""
        assert "B19013_001E" in ACS_VARIABLES
        assert isinstance(ACS_VARIABLES["B19013_001E"], str)

    def test_acs_variable_groups_has_income(self):
        """ACS_VARIABLE_GROUPS has income group."""
        assert "income" in ACS_VARIABLE_GROUPS
        assert "B19013_001E" in ACS_VARIABLE_GROUPS["income"]


class TestVariableFunctions:
    """Test the exported variable helper functions."""

    def test_get_variables_for_group(self):
        """get_variables_for_group returns list of variables."""
        vars = get_variables_for_group("housing")
        assert "H1_001N" in vars
        assert isinstance(vars, list)

    def test_get_variables_for_group_invalid(self):
        """get_variables_for_group raises for invalid group."""
        with pytest.raises(ValueError, match="Unknown variable group"):
            get_variables_for_group("nonexistent")

    def test_list_tables(self):
        """list_tables returns dict of table descriptions."""
        tables = list_tables()
        assert "P1" in tables
        assert "H1" in tables
        assert isinstance(tables["P1"], str)

    def test_list_variable_groups(self):
        """list_variable_groups returns dict of group descriptions."""
        groups = list_variable_groups()
        assert "population" in groups
        assert "housing" in groups
        assert isinstance(groups["population"], str)


class TestMultiBatchVariables:
    """Test that large variable sets work correctly (>50 per batch)."""

    async def test_many_acs_variables_triggers_multi_batch(
        self, mock_census_http, isolated_data_dir
    ):
        """Request >50 ACS variables to trigger multi-batch downloading."""
        # Get all ACS variables - there are ~145 of them
        all_acs_vars = list(ACS_VARIABLES.keys())
        assert len(all_acs_vars) > 50  # Verify we have enough to trigger batching

        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            acs_variables=all_acs_vars,
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        # Should have data for multiple variables from different batches
        assert result.census_data.get("B19013_001E") is not None  # Income (batch 1)
        assert result.census_data.get("B28002_001E") is not None  # Internet (later batch)


class TestACSVariableFunctions:
    """Test ACS variable helper functions."""

    def test_get_acs_variables_for_group(self):
        """get_acs_variables_for_group returns list of variables."""
        vars = get_acs_variables_for_group("income")
        assert "B19013_001E" in vars
        assert isinstance(vars, list)

    def test_get_acs_variables_for_group_invalid(self):
        """get_acs_variables_for_group raises for invalid group."""
        with pytest.raises(ValueError, match="Unknown ACS variable group"):
            get_acs_variables_for_group("nonexistent")

    def test_list_acs_tables(self):
        """list_acs_tables returns dict of table descriptions."""
        tables = list_acs_tables()
        assert isinstance(tables, dict)
        assert len(tables) > 0

    def test_list_acs_variable_groups(self):
        """list_acs_variable_groups returns dict of group descriptions."""
        groups = list_acs_variable_groups()
        assert "income" in groups
        assert "education" in groups
        assert isinstance(groups["income"], str)
