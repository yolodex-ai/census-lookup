"""Tests for ACS variable definitions and utilities."""

import pytest

from census_lookup.census.acs import (
    ACS_VARIABLE_GROUPS,
    ACS_VARIABLES,
    DEFAULT_ACS_VARIABLES,
    get_acs_api_endpoint,
    get_acs_variable_info,
    get_acs_variables_for_group,
    list_acs_tables,
    list_acs_variable_groups,
)


class TestACSVariables:
    """Tests for ACS variable definitions."""

    def test_variables_dict_not_empty(self):
        """Variables dictionary should contain entries."""
        assert len(ACS_VARIABLES) > 0

    def test_common_variables_exist(self):
        """Common ACS variables should be defined."""
        # Income
        assert "B19013_001E" in ACS_VARIABLES  # Median household income
        assert "B19301_001E" in ACS_VARIABLES  # Per capita income

        # Education
        assert "B15003_022E" in ACS_VARIABLES  # Bachelor's degree

        # Employment
        assert "B23025_004E" in ACS_VARIABLES  # Employed
        assert "B23025_005E" in ACS_VARIABLES  # Unemployed

        # Housing
        assert "B25077_001E" in ACS_VARIABLES  # Median home value
        assert "B25064_001E" in ACS_VARIABLES  # Median gross rent

    def test_variable_descriptions_are_strings(self):
        """All variable descriptions should be non-empty strings."""
        for code, desc in ACS_VARIABLES.items():
            assert isinstance(desc, str)
            assert len(desc) > 0, f"Variable {code} has empty description"

    def test_variable_codes_end_with_e(self):
        """All estimate variable codes should end with E."""
        for code in ACS_VARIABLES.keys():
            assert code.endswith("E"), f"Variable {code} should end with E"


class TestACSVariableGroups:
    """Tests for ACS variable groups."""

    def test_groups_dict_not_empty(self):
        """Groups dictionary should contain entries."""
        assert len(ACS_VARIABLE_GROUPS) > 0

    def test_expected_groups_exist(self):
        """Expected groups should be defined."""
        expected_groups = [
            "demographics",
            "income",
            "poverty",
            "education",
            "employment",
            "commute",
            "housing",
            "health_insurance",
            "household",
        ]
        for group in expected_groups:
            assert group in ACS_VARIABLE_GROUPS, f"Group {group} should exist"

    def test_group_values_are_lists(self):
        """Group values should be lists of strings."""
        for group, variables in ACS_VARIABLE_GROUPS.items():
            assert isinstance(variables, list), f"Group {group} should be a list"
            assert len(variables) > 0, f"Group {group} should not be empty"
            for var in variables:
                assert isinstance(var, str), f"Variables in {group} should be strings"


class TestGetACSVariableInfo:
    """Tests for get_acs_variable_info function."""

    def test_known_variable(self):
        """Should return info for known variable."""
        info = get_acs_variable_info("B19013_001E")
        assert info["code"] == "B19013_001E"
        assert "income" in info["description"].lower() or "household" in info["description"].lower()
        assert info["table"] == "B19013"
        assert info["is_estimate"] is True
        assert info["is_moe"] is False

    def test_unknown_variable(self):
        """Should return generic info for unknown variable."""
        info = get_acs_variable_info("UNKNOWN_001E")
        assert info["code"] == "UNKNOWN_001E"
        assert info["description"] == "Unknown variable"

    def test_moe_variable(self):
        """Should identify margin of error variables."""
        info = get_acs_variable_info("B19013_001M")
        assert info["is_moe"] is True
        assert info["is_estimate"] is False


class TestGetACSVariablesForGroup:
    """Tests for get_acs_variables_for_group function."""

    def test_valid_group(self):
        """Should return variables for valid group."""
        vars = get_acs_variables_for_group("income")
        assert len(vars) > 0
        assert "B19013_001E" in vars  # Median household income

    def test_education_group(self):
        """Education group should include attainment variables."""
        vars = get_acs_variables_for_group("education")
        assert "B15003_022E" in vars  # Bachelor's degree

    def test_invalid_group(self):
        """Should raise ValueError for invalid group."""
        with pytest.raises(ValueError, match="Unknown ACS variable group"):
            get_acs_variables_for_group("invalid_group")


class TestDefaultACSVariables:
    """Tests for default ACS variable list."""

    def test_not_empty(self):
        """Default list should not be empty."""
        assert len(DEFAULT_ACS_VARIABLES) > 0

    def test_contains_common_variables(self):
        """Should include commonly used variables."""
        assert "B19013_001E" in DEFAULT_ACS_VARIABLES  # Median income
        assert "B01001_001E" in DEFAULT_ACS_VARIABLES  # Total population

    def test_all_variables_are_valid(self):
        """All default variables should be in the main dictionary."""
        for var in DEFAULT_ACS_VARIABLES:
            assert var in ACS_VARIABLES, f"Default variable {var} not in ACS_VARIABLES"


class TestListFunctions:
    """Tests for listing functions."""

    def test_list_acs_tables(self):
        """Should return dictionary of table names."""
        tables = list_acs_tables()
        assert isinstance(tables, dict)
        assert len(tables) > 0
        # Check some expected tables
        assert "B01" in tables  # Sex and Age
        assert "B19" in tables  # Income

    def test_list_acs_variable_groups(self):
        """Should return dictionary of group descriptions."""
        groups = list_acs_variable_groups()
        assert isinstance(groups, dict)
        assert len(groups) > 0
        # Check expected groups
        assert "income" in groups
        assert "education" in groups


class TestACSAPIEndpoint:
    """Tests for API endpoint generation."""

    def test_default_year(self):
        """Default year should be 2020."""
        endpoint = get_acs_api_endpoint()
        assert "2020" in endpoint
        assert "acs5" in endpoint

    def test_custom_year(self):
        """Should accept custom year."""
        endpoint = get_acs_api_endpoint(year=2019)
        assert "2019" in endpoint

    def test_endpoint_format(self):
        """Endpoint should have correct format."""
        endpoint = get_acs_api_endpoint()
        assert endpoint.startswith("https://api.census.gov/data/")
        assert "/acs/acs5" in endpoint
