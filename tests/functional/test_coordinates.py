"""Functional tests for coordinate-based lookups.

Tests looking up census data by latitude/longitude coordinates.
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


def setup_acs_with_nulls_mocks(mocked: aioresponses) -> None:
    """Set up mocks with ACS returning null values."""
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

    # ACS returns data with null values
    def acs_callback(url, **kwargs):
        get_param = unquote(url.query.get("get", ""))
        if get_param:
            requested_vars = [v for v in get_param.split(",") if v.startswith("B")]
        else:
            requested_vars = ["B19013_001E"]

        # Return tract-level ACS data with null values
        header = ["GEO_ID", "NAME"] + requested_vars + ["state", "county", "tract"]
        rows = [header]

        # Return null for income variable
        geo_id = f"1400000US{TEST_TRACT_GEOID}"
        name = f"Census Tract {TEST_TRACT_GEOID[5:]}, DC"
        # Use None/null for values
        values = [None] * len(requested_vars)
        state = TEST_TRACT_GEOID[:2]
        county = TEST_TRACT_GEOID[2:5]
        tract_num = TEST_TRACT_GEOID[5:]

        rows.append([geo_id, name] + values + [state, county, tract_num])
        return CallbackResult(status=200, payload=rows)

    acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
    mocked.get(acs_pattern, callback=acs_callback, repeat=True)


class TestCoordinateLookup:
    """User can look up census data by coordinates."""

    async def test_coordinate_lookup(self, tmp_path: Path):
        """Look up census data for lat/lon coordinates."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            # Load DC by looking up an address first
            await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            # Now look up by coordinates (White House coordinates)
            result = await lookup.lookup_coordinates(38.8977, -77.0365)

            assert result.is_matched
            assert result.block is not None
            # Census data is nested by level
            assert "P1_001N" in result.census_data
            assert result.census_data["P1_001N"].get("block") is not None

    async def test_coordinate_batch_lookup(self, tmp_path: Path):
        """Batch coordinate lookup with DataFrame."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_standard_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            # Load DC first
            await lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

            # Create DataFrame with coordinates
            df = pd.DataFrame(
                {
                    "name": ["White House", "Capitol"],
                    "latitude": [38.8977, 38.8899],
                    "longitude": [-77.0365, -77.0091],
                }
            )

            results = await lookup.lookup_coordinates_batch(df)

            assert len(results) == 2
            assert "GEOID" in results.columns

    async def test_coordinate_lookup_with_acs_null_values(self, tmp_path: Path):
        """Coordinate lookup handles ACS null values correctly."""
        data_dir = setup_data_dir(tmp_path)

        with aioresponses() as mocked:
            setup_acs_with_nulls_mocks(mocked)

            lookup = CensusLookup(
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],  # Median income - will be null
                data_dir=data_dir,
            )
            # Load DC first
            await lookup.load_state("DC")

            # Look up by coordinates
            result = await lookup.lookup_coordinates(38.8977, -77.0365)

            assert result.is_matched
            # The ACS variable should have tract level with None value
            acs_data = result.census_data.get("B19013_001E")
            assert acs_data is None or acs_data.get("tract") is None
