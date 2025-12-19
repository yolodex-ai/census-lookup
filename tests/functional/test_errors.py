"""Functional tests for error handling.

Tests how the library handles invalid inputs and edge cases through the public API.
"""

import re
from pathlib import Path
from urllib.parse import unquote

import pytest
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


class TestInvalidStateErrors:
    """Test error handling for invalid state inputs."""

    async def test_invalid_state_fips_raises(self, tmp_path: Path):
        """Invalid FIPS code raises ValueError."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            with pytest.raises(ValueError, match="Unknown state"):
                await lookup.load_state("99")

    async def test_invalid_state_abbrev_raises(self, tmp_path: Path):
        """Invalid state abbreviation raises ValueError."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            with pytest.raises(ValueError, match="Unknown state"):
                await lookup.load_state("XX")

    async def test_invalid_state_name_raises(self, tmp_path: Path):
        """Invalid state name raises ValueError."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            with pytest.raises(ValueError, match="Unknown state"):
                await lookup.load_state("NotAState")


class TestErrorHandling:
    """Errors are handled gracefully."""

    async def test_invalid_address(self, tmp_path: Path):
        """Invalid address returns no_match result."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("this is not a valid address")

            assert not result.is_matched
            assert result.match_type in ["no_match", "no_state", "parse_error"]

    async def test_address_without_state(self, tmp_path: Path):
        """Address without state returns no_state."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("123 Main Street")

            assert not result.is_matched
            assert result.match_type in ["no_state", "parse_error"]

    async def test_empty_address(self, tmp_path: Path):
        """Empty address returns parse_error."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("")

            assert not result.is_matched
            assert result.match_type == "parse_error"

    async def test_whitespace_only_address(self, tmp_path: Path):
        """Whitespace-only address returns parse_error."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("   ")

            assert not result.is_matched
            assert result.match_type == "parse_error"
