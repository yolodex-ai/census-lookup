"""Functional tests for CensusLookup configuration.

Tests initialization, state loading, and variable management through the public API.
"""

import re
from pathlib import Path
from urllib.parse import unquote

from aioresponses import CallbackResult, aioresponses

from census_lookup import CensusLookup, GeoLevel
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


class TestLoadingAndConfiguration:
    """User can configure the lookup instance."""

    async def test_load_state_explicitly(self, tmp_path: Path):
        """Load state data explicitly before lookups."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            await lookup.load_state("DC")

            assert "11" in lookup.loaded_states

    async def test_load_state_by_full_name(self, tmp_path: Path):
        """Load state using full state name."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # Tests normalize_state with full name
            await lookup.load_state("District of Columbia")

            assert "11" in lookup.loaded_states

    async def test_load_state_by_fips(self, tmp_path: Path):
        """Load state using FIPS code."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            await lookup.load_state("11")

            assert "11" in lookup.loaded_states

    async def test_load_multiple_states(self, tmp_path: Path):
        """Load multiple states at once."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
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

    async def test_download_triggers_on_first_lookup(self, tmp_path: Path):
        """Data is downloaded automatically when needed."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # This should trigger download via mocked endpoints
            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            assert "11" in lookup.loaded_states

    async def test_default_variables(self, tmp_path: Path):
        """No variables specified uses default (P1_001N)."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            # Default should include at least population
            assert "P1_001N" in result.census_data
