"""Tests for census variable definitions."""

import pytest

from census_lookup.census.variables import (
    VARIABLES,
    VARIABLE_GROUPS,
    DEFAULT_VARIABLES,
    get_variable_info,
    get_variables_for_group,
    list_tables,
    list_variable_groups,
)


class TestVariables:
    """Tests for variable definitions."""

    def test_total_population_variable_exists(self):
        """Test that total population variable exists."""
        assert "P1_001N" in VARIABLES
        assert "population" in VARIABLES["P1_001N"].lower()

    def test_housing_variables_exist(self):
        """Test that housing variables exist."""
        assert "H1_001N" in VARIABLES  # Total housing
        assert "H1_002N" in VARIABLES  # Occupied
        assert "H1_003N" in VARIABLES  # Vacant

    def test_race_variables_exist(self):
        """Test that race variables exist."""
        assert "P1_003N" in VARIABLES  # White alone
        assert "P1_004N" in VARIABLES  # Black alone
        assert "P1_006N" in VARIABLES  # Asian alone

    def test_hispanic_variables_exist(self):
        """Test that Hispanic/Latino variables exist."""
        assert "P2_002N" in VARIABLES  # Hispanic or Latino

    def test_voting_age_variables_exist(self):
        """Test that voting age variables exist."""
        assert "P3_001N" in VARIABLES  # Population 18+


class TestVariableGroups:
    """Tests for variable groups."""

    def test_population_group(self):
        """Test population group."""
        assert "population" in VARIABLE_GROUPS
        assert "P1_001N" in VARIABLE_GROUPS["population"]

    def test_housing_group(self):
        """Test housing group."""
        assert "housing" in VARIABLE_GROUPS
        housing = VARIABLE_GROUPS["housing"]
        assert "H1_001N" in housing
        assert "H1_002N" in housing
        assert "H1_003N" in housing

    def test_race_simple_group(self):
        """Test simple race group."""
        assert "race_simple" in VARIABLE_GROUPS
        race = VARIABLE_GROUPS["race_simple"]
        assert "P1_001N" in race  # Total
        assert "P1_003N" in race  # White

    def test_all_group_contains_all_variables(self):
        """Test that 'all' group contains all variables."""
        assert "all" in VARIABLE_GROUPS
        assert set(VARIABLE_GROUPS["all"]) == set(VARIABLES.keys())


class TestDefaultVariables:
    """Tests for default variables."""

    def test_default_includes_population(self):
        """Test that defaults include population."""
        assert "P1_001N" in DEFAULT_VARIABLES

    def test_default_includes_housing(self):
        """Test that defaults include housing."""
        assert "H1_001N" in DEFAULT_VARIABLES

    def test_default_is_reasonable_size(self):
        """Test that default list is reasonable size."""
        # Should be comprehensive but not excessive
        assert 5 <= len(DEFAULT_VARIABLES) <= 20


class TestGetVariableInfo:
    """Tests for get_variable_info function."""

    def test_get_population_info(self):
        """Test getting info for population variable."""
        info = get_variable_info("P1_001N")

        assert info["code"] == "P1_001N"
        assert "population" in info["description"].lower()
        assert info["table"] == "P1"

    def test_get_housing_info(self):
        """Test getting info for housing variable."""
        info = get_variable_info("H1_001N")

        assert info["code"] == "H1_001N"
        assert info["table"] == "H1"

    def test_get_unknown_variable_info(self):
        """Test getting info for unknown variable."""
        info = get_variable_info("UNKNOWN")

        assert info["code"] == "UNKNOWN"
        assert "unknown" in info["description"].lower()


class TestGetVariablesForGroup:
    """Tests for get_variables_for_group function."""

    def test_get_population_group(self):
        """Test getting population group."""
        vars = get_variables_for_group("population")
        assert "P1_001N" in vars

    def test_get_housing_group(self):
        """Test getting housing group."""
        vars = get_variables_for_group("housing")
        assert len(vars) == 3  # H1_001N, H1_002N, H1_003N

    def test_get_unknown_group_raises(self):
        """Test that unknown group raises error."""
        with pytest.raises(ValueError, match="Unknown variable group"):
            get_variables_for_group("nonexistent_group")

    def test_error_message_includes_valid_groups(self):
        """Test that error message includes valid group names."""
        with pytest.raises(ValueError) as exc_info:
            get_variables_for_group("nonexistent")

        error_msg = str(exc_info.value)
        assert "population" in error_msg or "housing" in error_msg


class TestListFunctions:
    """Tests for list functions."""

    def test_list_tables(self):
        """Test listing available tables."""
        tables = list_tables()

        assert "P1" in tables
        assert "P2" in tables
        assert "H1" in tables
        assert "Race" in tables["P1"]

    def test_list_variable_groups(self):
        """Test listing variable groups."""
        groups = list_variable_groups()

        assert "population" in groups
        assert "housing" in groups
        assert isinstance(groups["population"], str)  # Should have description
