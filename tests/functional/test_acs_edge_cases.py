"""Tests for ACS edge cases using inline mocking."""

import re
from pathlib import Path
from urllib.parse import unquote

from aioresponses import CallbackResult, aioresponses

from census_lookup import CensusLookup, GeoLevel

# Import helpers from conftest
from tests.functional.conftest import (
    DC_COUNTY_FIPS,
    DC_STATE_FIPS,
    TEST_TRACT_GEOID,
    create_dc_addrfeat_gdf,
    create_dc_blocks_gdf,
    create_dc_census_df,
    create_pl94171_zip,
    create_shapefile_zip,
)


class TestACSEdgeCases:
    """Test ACS data retrieval edge cases."""

    async def test_acs_empty_row_returns_no_acs_data(self, tmp_path: Path):
        """When ACS returns no matching tract, ACS data is empty.

        Tests line 337->346 (acs_row.empty branch).
        """
        blocks_gdf = create_dc_blocks_gdf()
        blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")
        addrfeat_gdf = create_dc_addrfeat_gdf()
        addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
        census_df = create_dc_census_df()
        pl94171_zip = create_pl94171_zip("dc", census_df)

        data_dir = tmp_path / "census-lookup"
        data_dir.mkdir()
        for subdir in ["tiger/blocks", "tiger/addrfeat", "census/pl94171", "census/acs", "temp"]:
            (data_dir / subdir).mkdir(parents=True)

        with aioresponses() as mocked:
            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

            addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            # Mock PL 94-171 bulk file download
            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # Mock ACS API to return data for a DIFFERENT tract (not our test area)
            def acs_callback_wrong_tract(url, **kwargs):
                get_param = unquote(url.query.get("get", ""))
                acs_vars = [v for v in get_param.split(",") if v.startswith("B")]
                requested_vars = acs_vars or ["B19013_001E"]

                # Return data for a different tract that won't match
                wrong_tract = "11001999999"  # Non-existent tract
                header = ["GEO_ID", "NAME"] + requested_vars + ["state", "county", "tract"]
                rows = [
                    header,
                    [f"1400000US{wrong_tract}", "Wrong Tract", "99999", "11", "001", "999999"],
                ]
                return CallbackResult(status=200, payload=rows)

            acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
            mocked.get(acs_pattern, callback=acs_callback_wrong_tract, repeat=True)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

            assert result.is_matched
            assert result.census_data.get("P1_001N") is not None
            # ACS data should be missing because tract didn't match
            assert result.census_data.get("B19013_001E") is None

    async def test_acs_variable_not_in_columns(self, tmp_path: Path):
        """When ACS response doesn't include requested variable column.

        Tests line 339->338 (variable not in acs_row.columns).
        """
        blocks_gdf = create_dc_blocks_gdf()
        blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")
        addrfeat_gdf = create_dc_addrfeat_gdf()
        addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
        census_df = create_dc_census_df()
        pl94171_zip = create_pl94171_zip("dc", census_df)

        data_dir = tmp_path / "census-lookup"
        data_dir.mkdir()
        for subdir in ["tiger/blocks", "tiger/addrfeat", "census/pl94171", "census/acs", "temp"]:
            (data_dir / subdir).mkdir(parents=True)

        with aioresponses() as mocked:
            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

            addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            # Mock PL 94-171 bulk file download
            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # Mock ACS API to return DIFFERENT variables than requested
            def acs_callback_missing_var(url, **kwargs):
                # Return only B19301_001E even though B19013_001E was requested
                header = ["GEO_ID", "NAME", "B19301_001E", "state", "county", "tract"]
                rows = [
                    header,
                    [f"1400000US{TEST_TRACT_GEOID}", "Test Tract", "55000", "11", "001", "006202"],
                ]
                return CallbackResult(status=200, payload=rows)

            acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
            mocked.get(acs_pattern, callback=acs_callback_missing_var, repeat=True)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],  # Request this, but API returns B19301_001E
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

            assert result.is_matched
            assert result.census_data.get("P1_001N") is not None
            # B19013_001E wasn't in the response columns
            assert result.census_data.get("B19013_001E") is None

    async def test_acs_null_value_becomes_none(self, tmp_path: Path):
        """When ACS returns null/NaN value, it becomes None in result.

        Tests line 341 (pd.notna check).
        """
        blocks_gdf = create_dc_blocks_gdf()
        blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")
        addrfeat_gdf = create_dc_addrfeat_gdf()
        addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
        census_df = create_dc_census_df()
        pl94171_zip = create_pl94171_zip("dc", census_df)

        data_dir = tmp_path / "census-lookup"
        data_dir.mkdir()
        for subdir in ["tiger/blocks", "tiger/addrfeat", "census/pl94171", "census/acs", "temp"]:
            (data_dir / subdir).mkdir(parents=True)

        with aioresponses() as mocked:
            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

            addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            # Mock PL 94-171 bulk file download
            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # Mock ACS API to return null value
            def acs_callback_null_value(url, **kwargs):
                header = ["GEO_ID", "NAME", "B19013_001E", "state", "county", "tract"]
                rows = [
                    header,
                    # Return null for the income variable
                    [f"1400000US{TEST_TRACT_GEOID}", "Test Tract", None, "11", "001", "006202"],
                ]
                return CallbackResult(status=200, payload=rows)

            acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
            mocked.get(acs_pattern, callback=acs_callback_null_value, repeat=True)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],
                data_dir=data_dir,
            )

            result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 20500")

            assert result.is_matched
            assert result.census_data.get("P1_001N") is not None
            # Null value should be converted to None
            assert result.census_data.get("B19013_001E") is None
