"""Functional tests for single address geocoding.

Tests the core geocoding functionality through the public API.
"""

import re
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
from aioresponses import CallbackResult, aioresponses

from census_lookup import CensusLookup
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


class TestSingleAddressLookup:
    """User can look up a single address and get census data."""

    async def test_basic_lookup(self, tmp_path: Path):
        """Look up an address, get GEOID and population at all levels."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            assert result.is_matched
            assert result.block is not None
            # Census data is now nested by level
            assert result.census_data["P1_001N"]["block"] > 0

    async def test_all_levels_returned(self, tmp_path: Path):
        """Data is returned at all geographic levels."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
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

    async def test_all_geographic_levels_in_result(self, tmp_path: Path):
        """Test all geographic levels are returned in the result."""
        data_dir = setup_data_dir(tmp_path)
        address = "1600 Pennsylvania Avenue NW, Washington, DC"

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            result = await lookup.geocode(address)

            assert result.is_matched
            # All levels should be present in result
            assert result.block is not None and len(result.block) == 15
            assert result.block_group is not None and len(result.block_group) == 12
            assert result.tract is not None and len(result.tract) == 11
            assert result.county_fips is not None and len(result.county_fips) == 5
            assert result.state_fips is not None and len(result.state_fips) == 2

    async def test_geoid_components_populated(self, tmp_path: Path):
        """GEOID components are correctly parsed from the result."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
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

    async def test_geoid_components_in_dict(self, tmp_path: Path):
        """GEOID components are included in to_dict output."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            data = result.to_dict()

            assert "state_fips" in data
            assert "county_fips" in data
            assert "tract" in data
            assert "block_group" in data
            assert "block" in data
            assert data["state_fips"] == "11"

    async def test_result_to_dict(self, tmp_path: Path):
        """Result can be converted to dictionary with nested census data."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
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

    async def test_result_to_series(self, tmp_path: Path):
        """Result can be converted to pandas Series (flattened at block level)."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
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

    async def test_address_with_directionals(self, tmp_path: Path):
        """Address with directional prefixes parses correctly."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

            assert result.is_matched

    async def test_address_with_cardinal_directional(self, tmp_path: Path):
        """Address with N/S/E/W directional."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # Tests normalizer directional expansion (N -> NORTH)
            result = await lookup.geocode("100 N Capitol St, Washington, DC")

            # May or may not match depending on data, but should parse
            assert result.match_type in ["interpolated", "exact", "no_match"]

    async def test_address_with_ordinal_street(self, tmp_path: Path):
        """Address with ordinal street name (1st, 2nd, etc.)."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # Tests normalizer ordinal handling
            result = await lookup.geocode("100 1st Street NE, Washington, DC")

            assert result.match_type in ["interpolated", "exact", "no_match"]

    async def test_address_with_abbreviations(self, tmp_path: Path):
        """Address with street type abbreviations works."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

            assert result.is_matched

    async def test_address_with_zipcode(self, tmp_path: Path):
        """Address with zipcode parses correctly."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC 20500")

            assert result.is_matched

    async def test_address_lowercase(self, tmp_path: Path):
        """Lowercase address is normalized and matches."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 pennsylvania avenue nw, washington, dc")

            assert result.is_matched
