"""Functional tests for batch geocoding.

Tests batch processing of multiple addresses through the public API.
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


class TestBatchLookup:
    """User can geocode multiple addresses at once."""

    async def test_batch_geocoding(self, tmp_path: Path):
        """Batch geocode returns DataFrame with flattened results."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # Pre-load DC to avoid race condition in concurrent batch geocoding
            await lookup.load_state("DC")

            results = await lookup.geocode_batch(
                [
                    "1600 Pennsylvania Avenue NW, Washington, DC",
                    "100 Maryland Ave SW, Washington, DC",
                ]
            )

            assert len(results) == 2
            # Batch output has all GEOIDs as flat columns
            assert "block" in results.columns
            # Census data is flattened at output_level (default: block)
            assert "P1_001N" in results.columns

    async def test_batch_from_series(self, tmp_path: Path):
        """Batch accepts pandas Series as input."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            # Pre-load DC to avoid race condition in concurrent batch geocoding
            await lookup.load_state("DC")

            addresses = pd.Series(
                [
                    "1600 Pennsylvania Avenue NW, Washington, DC",
                    "100 Maryland Ave SW, Washington, DC",
                ]
            )

            results = await lookup.geocode_batch(addresses, progress=False)

            assert len(results) == 2
            assert "block" in results.columns

    async def test_batch_with_unmatched(self, tmp_path: Path):
        """Batch handles unmatched addresses gracefully."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )

            results = await lookup.geocode_batch(
                [
                    "1600 Pennsylvania Avenue NW, Washington, DC",
                    "completely invalid address that won't match",
                ]
            )

            assert len(results) == 2
            # At least one should be matched
            assert results["match_type"].isin(["interpolated", "exact"]).sum() >= 1
