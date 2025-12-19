"""Functional tests for census variables.

Tests PL 94-171 variables, ACS variables, and variable groups through the public API.
"""

import re
from pathlib import Path
from urllib.parse import unquote

import pytest
from aioresponses import CallbackResult, aioresponses

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
from tests.functional.conftest import (
    DC_COUNTY_FIPS,
    DC_STATE_FIPS,
    TEST_TRACT_GEOID,
    create_acs_api_response,
    create_dc_addrfeat_gdf,
    create_dc_blocks_gdf,
    create_dc_census_df,
    create_pl94171_zip,
    create_shapefile_zip,
)


def setup_data_dir(tmp_path: Path) -> Path:
    """Create an isolated data directory for tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


def setup_standard_mocks(mocked: aioresponses) -> None:
    """Set up standard mocks for TIGER and Census endpoints."""
    blocks_gdf = create_dc_blocks_gdf()
    addrfeat_gdf = create_dc_addrfeat_gdf()
    census_df = create_dc_census_df()

    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")
    pl94171_zip = create_pl94171_zip("dc", census_df)

    # Mock TIGER block downloads
    blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
    mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

    # Mock TIGER address feature downloads
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
    mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

    # Mock PL 94-171 bulk file downloads
    pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
    mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

    # Mock ACS API
    def acs_callback(url, **kwargs):
        get_param = unquote(url.query.get("get", ""))
        if get_param:
            requested_vars = [v for v in get_param.split(",") if v.startswith("B")]
        else:
            requested_vars = ["B19013_001E"]

        response = create_acs_api_response(
            requested_vars,
            [TEST_TRACT_GEOID],
        )
        return CallbackResult(
            status=200,
            payload=response,
        )

    acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
    mocked.get(acs_pattern, callback=acs_callback, repeat=True)


def setup_many_acs_variables_mocks(mocked: aioresponses) -> None:
    """Set up mocks that handle many ACS variables (>50) with batch merging."""
    blocks_gdf = create_dc_blocks_gdf()
    addrfeat_gdf = create_dc_addrfeat_gdf()
    census_df = create_dc_census_df()

    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")
    pl94171_zip = create_pl94171_zip("dc", census_df)

    # Mock TIGER block downloads
    blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
    mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

    # Mock TIGER address feature downloads
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
    mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

    # Mock PL 94-171 bulk file downloads
    pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
    mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

    # ACS handles many variables
    def acs_callback(url, **kwargs):
        get_param = unquote(url.query.get("get", ""))
        if get_param:
            parts = get_param.split(",")
            requested_vars = [v for v in parts if v.startswith("B")]
        else:
            requested_vars = ["B19013_001E"]

        # Return tract-level ACS data with all requested variables
        tracts = [TEST_TRACT_GEOID]
        header = ["GEO_ID", "NAME"] + requested_vars + ["state", "county", "tract"]
        rows = [header]

        for tract in tracts:
            geo_id = f"1400000US{tract}"
            name = f"Census Tract {tract[5:]}, DC"
            values = ["50000"] * len(requested_vars)
            state = tract[:2]
            county = tract[2:5]
            tract_num = tract[5:]
            rows.append([geo_id, name] + values + [state, county, tract_num])

        return CallbackResult(status=200, payload=rows)

    acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
    mocked.get(acs_pattern, callback=acs_callback, repeat=True)


class TestACSData:
    """User can retrieve ACS data (income, education, etc.)."""

    async def test_acs_income_data(self, tmp_path: Path):
        """Get median household income from ACS."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                acs_variables=["B19013_001E"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            assert result.census_data.get("B19013_001E") is not None

            # Properly close to clean up ACS session
            await lookup.close()

    async def test_combined_pl94171_and_acs(self, tmp_path: Path):
        """Get both PL 94-171 (population) and ACS (income) together."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert "P1_001N" in result.census_data
            assert "B19013_001E" in result.census_data

    async def test_acs_variable_groups(self, tmp_path: Path):
        """Use ACS variable groups instead of individual variables."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                acs_variable_groups=["income"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            # Income group should include median household income
            assert result.census_data.get("B19013_001E") is not None


class TestVariableGroups:
    """User can use variable groups instead of individual codes."""

    async def test_population_group(self, tmp_path: Path):
        """Use 'population' group to get all population variables."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variable_groups=["population"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            assert "P1_001N" in result.census_data

    async def test_housing_group(self, tmp_path: Path):
        """Use 'housing' group to get housing variables."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variable_groups=["housing"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            assert "H1_001N" in result.census_data

    async def test_combined_groups_and_variables(self, tmp_path: Path):
        """Combine variable groups with individual variables."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_003N"],
                variable_groups=["housing"],
                data_dir=data_dir,
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

    async def test_many_acs_variables_triggers_multi_batch(self, tmp_path: Path):
        """Request >50 ACS variables to trigger multi-batch downloading."""
        data_dir = setup_data_dir(tmp_path)

        # Get all ACS variables - there are ~145 of them
        all_acs_vars = list(ACS_VARIABLES.keys())
        assert len(all_acs_vars) > 50  # Verify we have enough to trigger batching

        with aioresponses() as mocked:
            setup_many_acs_variables_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                acs_variables=all_acs_vars,
                data_dir=data_dir,
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
