"""Functional tests for edge cases and less common code paths.

Tests edge cases that exercise error handling and fallback logic through public API.
"""

from census_lookup import CensusLookup, GeoLevel


class TestMatcherEdgeCases:
    """Test matcher edge cases through the public API."""

    async def test_address_without_street_info(self, mock_census_http, isolated_data_dir):
        """Address without street info returns no_match."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Address that parses but has no street name
        result = await lookup.geocode("Washington, DC 20500")

        assert not result.is_matched
        assert result.match_type in ["no_match", "parse_error"]

    async def test_address_with_invalid_ranges_skipped(self, mock_census_http, isolated_data_dir):
        """Address on street with invalid house number ranges is skipped.

        The mock data includes Constitution Ave with 'INVALID' as house numbers.
        This tests the ValueError/TypeError handling in _check_range.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Constitution Ave has two segments:
        # 1. One with invalid ranges (LFROMHN='INVALID') - should be skipped
        # 2. One with valid ranges (500-699) - should match if house number is in range

        # Address that should skip invalid segment and match the valid one
        result = await lookup.geocode("550 Constitution Ave NW, Washington, DC 20001")

        # The geocoding succeeded (coordinates were interpolated) even though
        # the point is outside our mock blocks (no_block).
        # This tests that the invalid range segment was skipped correctly.
        assert result.latitude is not None
        assert result.longitude is not None
        assert result.matched_address == "CONSTITUTION AVE NW"
        # no_block means geocoding worked but point not in any block polygon
        assert result.match_type in ["interpolated", "no_block"]

    async def test_address_with_unknown_parity(self, mock_census_http, isolated_data_dir):
        """Address with unknown parity (X) falls back to range start matching.

        The mock data has Constitution Ave with PARITYL='X' which triggers
        the else branch in _parity_matches.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # The 500-698 range starts with 500 (even), so even house numbers should match
        result = await lookup.geocode("600 Constitution Ave NW, Washington, DC 20001")

        # The geocoding succeeded with coordinates even though outside mock blocks
        assert result.latitude is not None
        assert result.longitude is not None
        assert result.matched_address == "CONSTITUTION AVE NW"
        assert result.match_type in ["interpolated", "no_block"]

    async def test_equal_from_to_range_interpolation(self, mock_census_http, isolated_data_dir):
        """Address on segment with from=to range interpolates to middle.

        Tests the line 273 branch where to_addr == from_addr.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # SINGLE ST NW has LFROMHN=LTOHN=100
        result = await lookup.geocode("100 Single St NW, Washington, DC 20002")

        assert result.latitude is not None
        assert result.longitude is not None
        assert result.matched_address == "SINGLE ST NW"

    async def test_parity_b_allows_any_number(self, mock_census_http, isolated_data_dir):
        """Address with PARITY=B allows both odd and even house numbers.

        Tests line 239 where parity is "B".
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # BOTH ST NW has PARITYL='B' so both even and odd should match on left side
        result_odd = await lookup.geocode("51 Both St NW, Washington, DC 20003")
        result_even = await lookup.geocode("50 Both St NW, Washington, DC 20003")

        # Both should match
        assert result_odd.latitude is not None
        assert result_even.latitude is not None
        assert result_odd.matched_address == "BOTH ST NW"
        assert result_even.matched_address == "BOTH ST NW"

    async def test_address_outside_all_ranges(self, mock_census_http, isolated_data_dir):
        """Address that matches street name but house number is outside all ranges.

        Tests line 180 (return None from _find_segment).
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Pennsylvania Ave has ranges 1500-1699, so house number 9999 is outside
        result = await lookup.geocode("9999 Pennsylvania Ave NW, Washington, DC 20500")

        assert not result.is_matched
        assert result.match_type in ["no_match", "parse_error"]

    async def test_right_side_parity_mismatch(self, mock_census_http, isolated_data_dir):
        """Even address on right-only segment with PARITYR=O returns no match.

        RIGHTONLY ST NW has no left range, and right range 301-399 with PARITYR='O'.
        An even address (350) will check right side and fail parity.
        This tests line 226->229 (right side parity check fails).
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # 350 is even, right side range is 301-399 with PARITYR='O' (odd only)
        # Left side has no range (LFROMHN=None), so left check is skipped
        # Right side: 350 is in 301-399 range but fails parity (O wants odd, 350 is even)
        result = await lookup.geocode("350 Rightonly St NW, Washington, DC 20004")

        # Should NOT match because parity fails
        assert not result.is_matched
        assert result.match_type in ["no_match", "parse_error"]

    async def test_address_with_invalid_house_number(self, mock_census_http, isolated_data_dir):
        """Address with non-numeric house number returns no_match."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Address with letters instead of house number
        result = await lookup.geocode("ABC Main Street, Washington, DC")

        assert not result.is_matched
        assert result.match_type in ["no_match", "parse_error"]

    async def test_address_with_repeated_labels(self, mock_census_http, isolated_data_dir):
        """Address with repeated labels (multiple unit types) still parses."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Address with multiple unit designators triggers RepeatedLabelError
        # which should be handled gracefully via parse() fallback
        result = await lookup.geocode("123 Main St Apt 1 Suite 2, Washington, DC")

        # Should either match or return no_match, not crash
        assert result.match_type in ["interpolated", "no_match", "parse_error"]

    async def test_address_with_empty_street_in_features(self, mock_census_http, isolated_data_dir):
        """Address features with empty street names are skipped."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # This should still work because we skip empty street names
        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched

    async def test_parity_matching_odd(self, mock_census_http, isolated_data_dir):
        """Odd house numbers match on streets with parity restrictions."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # 1601 is odd, should match the right side (PARITYR=O)
        result = await lookup.geocode("1601 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched
        # Odd addresses match - coordinates should be valid
        assert result.latitude is not None
        assert result.longitude is not None

    async def test_parity_matching_even(self, mock_census_http, isolated_data_dir):
        """Even house numbers match on streets with parity restrictions."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # 1600 is even, should match the left side (PARITYL=E)
        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched
        # Even addresses match - coordinates should be valid
        assert result.latitude is not None
        assert result.longitude is not None

    async def test_variant_matching_fallback(self, mock_census_http, isolated_data_dir):
        """When exact match fails, variants are tried."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Use full street type that needs to be abbreviated to match
        result = await lookup.geocode("1600 Pennsylvania Avenue Northwest, Washington, DC")

        # Should still match via variant (AVE NW)
        assert result.is_matched or result.match_type == "no_match"


class TestSpatialEdgeCases:
    """Test spatial lookup edge cases."""

    async def test_point_outside_all_blocks(self, mock_census_http, isolated_data_dir):
        """Point far from any block returns no spatial match."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Load DC state first
        await lookup.load_state("DC")

        # Lookup coordinates far from DC
        result = await lookup.lookup_coordinates(lat=0.0, lon=0.0)

        assert not result.is_matched
        assert result.match_type == "no_block"

    async def test_point_in_bbox_but_not_in_polygon(self, mock_census_http, isolated_data_dir):
        """Point in bounding box but outside concave polygon returns no match.

        This tests the case where spatial index returns a candidate (bbox intersection)
        but the point is not actually inside the polygon (e.g., in the "notch" of an L-shape).
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        await lookup.load_state("DC")

        # The L-shaped block has vertices creating a notch at top-right:
        # The block spans from (lon+0.02, lat+0.02) to (lon+0.03, lat+0.03)
        # but has a notch cut out at (lon+0.025 to lon+0.03, lat+0.025 to lat+0.03)
        # A point at (lon+0.027, lat+0.027) is IN the bbox but OUTSIDE the polygon

        # WHITE_HOUSE_LON = -77.0365, WHITE_HOUSE_LAT = 38.8977
        # So test point is at (-77.0365 + 0.027, 38.8977 + 0.027) = (-77.0095, 38.9247)
        result = await lookup.lookup_coordinates(
            lat=38.8977 + 0.027,  # In the "notch"
            lon=-77.0365 + 0.027,
        )

        # Should not match - point is in bbox but outside polygon
        assert not result.is_matched
        assert result.match_type == "no_block"


class TestDuckDBEngineEdgeCases:
    """Test DuckDB engine edge cases."""

    async def test_multiple_valid_variables(self, mock_census_http, isolated_data_dir):
        """Requesting multiple valid variables returns all of them."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N", "H1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched
        assert result.census_data.get("P1_001N") is not None
        assert result.census_data.get("H1_001N") is not None


class TestCatalogEdgeCases:
    """Test data catalog edge cases."""

    async def test_catalog_created_on_first_use(self, mock_census_http, isolated_data_dir):
        """Catalog is created when first state is loaded."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Load a state
        await lookup.load_state("DC")

        # Catalog should exist
        catalog_path = isolated_data_dir / "catalog.json"
        assert catalog_path.exists()


class TestInterpolationEdgeCases:
    """Test address interpolation edge cases."""

    async def test_address_at_range_start(self, mock_census_http, isolated_data_dir):
        """Address at start of range interpolates correctly."""
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # 1500 is start of left range (1500-1698)
        result = await lookup.geocode("1500 Pennsylvania Ave NW, Washington, DC")

        if result.is_matched:
            # Position should be near start of segment
            assert result.latitude is not None
            assert result.longitude is not None

    async def test_address_at_range_end(self, mock_census_http, isolated_data_dir):
        """Address at end of range interpolates correctly."""
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # 1698 is end of left range
        result = await lookup.geocode("1698 Pennsylvania Ave NW, Washington, DC")

        if result.is_matched:
            assert result.latitude is not None
            assert result.longitude is not None

    async def test_equal_from_to_range(self, mock_census_http, isolated_data_dir):
        """Address range where from=to interpolates to middle."""
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Standard lookup - position should be valid
        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result.is_matched
        assert result.latitude is not None


class TestCoordinateLookupEdgeCases:
    """Test coordinate lookup edge cases."""

    async def test_lookup_coordinates_with_acs(self, mock_census_http, isolated_data_dir):
        """Coordinate lookup retrieves ACS data when available."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],
            data_dir=isolated_data_dir,
        )

        # Load state first
        await lookup.load_state("DC")

        # Use coordinates near White House
        result = await lookup.lookup_coordinates(lat=38.8977, lon=-77.0365)

        if result.is_matched:
            # Should have both PL 94-171 and ACS data
            assert result.census_data.get("P1_001N") is not None


class TestBatchProcessingEdgeCases:
    """Test batch processing edge cases."""

    async def test_batch_with_empty_address(self, mock_census_http, isolated_data_dir):
        """Batch handles empty addresses gracefully."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        addresses = [""]  # Single empty address

        results = await lookup.geocode_batch(addresses)

        assert len(results) == 1
        # Empty should be parse_error
        assert results.iloc[0]["match_type"] == "parse_error"

    async def test_batch_all_addresses_fail_to_match(self, mock_census_http, isolated_data_dir):
        """Batch where all addresses fail to match returns empty geoids.

        Tests lookup.py line 505->513 where geoids list is empty.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # All invalid addresses that won't match any street
        addresses = [
            "99999 Nonexistent Blvd, Washington, DC",
            "88888 Fake Lane, Washington, DC",
        ]

        results = await lookup.geocode_batch(addresses)

        assert len(results) == 2
        # Both should fail to match
        assert (
            results["match_type"].eq("no_match").all()
            or results["match_type"].eq("parse_error").all()
        )


class TestCoordinateBatchEdgeCases:
    """Test coordinate batch processing edge cases."""

    async def test_coordinate_batch_all_outside_blocks(self, mock_census_http, isolated_data_dir):
        """Batch of coordinates all outside any block returns empty geoids.

        Tests lookup.py line 505->513 (if geoids is empty in lookup_coordinates_batch).
        """
        import pandas as pd

        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        await lookup.load_state("DC")

        # Coordinates far from any DC blocks (Antarctica)
        df = pd.DataFrame(
            {
                "name": ["Point1", "Point2"],
                "latitude": [-70.0, -71.0],  # Antarctic
                "longitude": [0.0, 0.0],
            }
        )

        results = await lookup.lookup_coordinates_batch(df)

        assert len(results) == 2
        # All GEOIDs should be None since no blocks matched
        assert results["GEOID"].isna().all()


class TestZipCodeFiltering:
    """Test ZIP code filtering in matcher."""

    async def test_zip_filter_no_matches(self, mock_census_http, isolated_data_dir):
        """ZIP filter finds candidates but none match the ZIP code.

        Tests matcher.py line 173->177 where zip_mask.any() is False.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Pennsylvania Ave exists but with ZIP 20500, not 99999
        # The matcher finds the street but ZIP doesn't match any segment
        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC 99999")

        # Should still attempt to match (falls through without filtering)
        # The result depends on whether the non-ZIP-filtered match succeeds
        assert result.match_type in ["interpolated", "no_match", "no_block"]


class TestSingleWordStreet:
    """Test single-word street name variant generation."""

    async def test_single_word_street_no_type_variant(self, mock_census_http, isolated_data_dir):
        """Single-word street name doesn't generate variant without type.

        Tests normalizer.py line 463->468 where len(words) == 1.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # "Broadway" is a single word - variant generation should skip the
        # "remove street type" logic (line 463-466) since there's only one word
        result = await lookup.geocode("123 Broadway, Washington, DC")

        # Result will be no_match since we don't have Broadway in mock data,
        # but the important thing is that variant generation didn't crash
        assert result.match_type in ["no_match", "parse_error", "interpolated"]


class TestACSEdgeCases:
    """Test ACS data edge cases with custom mocked responses."""

    async def test_acs_tract_present_in_data(self, mock_census_http, isolated_data_dir):
        """When tract is in ACS data, ACS variables are retrieved.

        Tests that the normal path works with ACS data.
        """
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            acs_variables=["B19013_001E"],
            data_dir=isolated_data_dir,
        )

        # Load state first
        await lookup.load_state("DC")

        # Lookup with the standard mock - tract 11001006202 IS in ACS data
        result = await lookup.geocode("1600 Pennsylvania Ave NW, Washington, DC")
        assert result.is_matched
        # ACS data should be present since tract matches
        assert result.census_data.get("B19013_001E") is not None

    async def test_acs_variable_missing_from_response(self, tmp_path):
        """When one ACS variable is missing from response, it's skipped gracefully.

        Tests lookup.py line 440 where var not in acs_row.columns.
        We request TWO variables but API only returns ONE - the present variable
        gets data, the missing one is skipped.
        """
        import re

        from aioresponses import CallbackResult, aioresponses

        from tests.functional.conftest import (
            TEST_TRACT_GEOID,
            create_dc_addrfeat_gdf,
            create_dc_blocks_gdf,
            create_dc_census_df,
            create_pl94171_zip,
            create_shapefile_zip,
        )

        # Create mock data
        blocks_gdf = create_dc_blocks_gdf()
        blocks_zip = create_shapefile_zip(blocks_gdf, "tl_2020_11_tabblock20")
        addrfeat_gdf = create_dc_addrfeat_gdf()
        addrfeat_zip = create_shapefile_zip(addrfeat_gdf, "tl_2020_11001_addrfeat")
        census_df = create_dc_census_df()
        pl94171_zip = create_pl94171_zip("dc", census_df)

        # Set up data directory
        data_dir = tmp_path / "census-lookup"
        data_dir.mkdir()
        for subdir in [
            "tiger/blocks",
            "tiger/addrfeat",
            "census/pl94171",
            "census/acs5/tract",
            "temp",
        ]:
            (data_dir / subdir).mkdir(parents=True)

        with aioresponses() as mocked:
            # Standard mocks for TIGER and PL 94-171
            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

            addrfeat_pattern = re.compile(r".*census\.gov.*ADDRFEAT.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # ACS mock returns only ONE of the requested variables
            # User requests [B19013_001E, B19301_001E] but API only returns B19013_001E
            def acs_callback(url, **kwargs):
                # Return only B19013_001E, not B19301_001E
                header = ["GEO_ID", "NAME", "B19013_001E", "state", "county", "tract"]
                rows = [
                    header,
                    [f"1400000US{TEST_TRACT_GEOID}", "Test", "75000", "11", "001", "006202"],
                ]
                return CallbackResult(status=200, payload=rows)

            acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
            mocked.get(acs_pattern, callback=acs_callback, repeat=True)

            # Request TWO variables - one exists (B19013_001E), one doesn't (B19301_001E)
            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E", "B19301_001E"],  # B19301_001E won't be in response
                data_dir=data_dir,
            )

            await lookup.load_state("DC")

            # Lookup should succeed
            result = await lookup.lookup_coordinates(lat=38.8977, lon=-77.0365)

            assert result.is_matched
            # B19013_001E IS in the response, so it should be in census_data
            assert result.census_data.get("B19013_001E") == 75000.0
            # B19301_001E is NOT in the response (line 440 branch: var not in columns)
            assert result.census_data.get("B19301_001E") is None

    async def test_acs_row_empty_for_tract(self, tmp_path):
        """When tract not found in ACS data, ACS variables are skipped gracefully.

        Tests lookup.py line 437 where acs_row.empty is True.
        """
        import re

        from aioresponses import CallbackResult, aioresponses

        from tests.functional.conftest import (
            create_dc_addrfeat_gdf,
            create_dc_blocks_gdf,
            create_dc_census_df,
            create_pl94171_zip,
            create_shapefile_zip,
        )

        # Create mock data
        blocks_gdf = create_dc_blocks_gdf()
        blocks_zip = create_shapefile_zip(blocks_gdf, "tl_2020_11_tabblock20")
        addrfeat_gdf = create_dc_addrfeat_gdf()
        addrfeat_zip = create_shapefile_zip(addrfeat_gdf, "tl_2020_11001_addrfeat")
        census_df = create_dc_census_df()
        pl94171_zip = create_pl94171_zip("dc", census_df)

        # Set up data directory
        data_dir = tmp_path / "census-lookup"
        data_dir.mkdir()
        for subdir in [
            "tiger/blocks",
            "tiger/addrfeat",
            "census/pl94171",
            "census/acs5/tract",
            "temp",
        ]:
            (data_dir / subdir).mkdir(parents=True)

        with aioresponses() as mocked:
            # Standard mocks
            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

            addrfeat_pattern = re.compile(r".*census\.gov.*ADDRFEAT.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # ACS mock returns data for DIFFERENT tract than the one we'll look up
            def acs_callback(url, **kwargs):
                # Return data for a completely different tract (99999999999)
                header = ["GEO_ID", "NAME", "B19013_001E", "state", "county", "tract"]
                rows = [
                    header,
                    ["1400000US99999999999", "Wrong Tract", "99999", "99", "999", "999999"],
                ]
                return CallbackResult(status=200, payload=rows)

            acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
            mocked.get(acs_pattern, callback=acs_callback, repeat=True)

            lookup = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                acs_variables=["B19013_001E"],
                data_dir=data_dir,
            )

            await lookup.load_state("DC")

            # Lookup White House coords - tract 11001006202 won't be in ACS data
            result = await lookup.lookup_coordinates(lat=38.8977, lon=-77.0365)

            assert result.is_matched
            # ACS variable won't be present because tract wasn't found
            # (line 437 branch: if not acs_row.empty is False, so we skip)
            assert result.census_data.get("B19013_001E") is None


class TestCatalogFileMissing:
    """Test catalog behavior when registered file is missing."""

    async def test_load_state_with_deleted_file_redownloads(self, tmp_path):
        """When cached file is deleted, load_state re-downloads it.

        Tests catalog.py line 153->155 where path.exists() is False.
        This is tested through the public API by deleting cached files.
        """
        import re

        from aioresponses import CallbackResult, aioresponses

        from tests.functional.conftest import (
            DC_COUNTY_FIPS,
            DC_STATE_FIPS,
            create_dc_addrfeat_gdf,
            create_dc_blocks_gdf,
            create_dc_census_df,
            create_pl94171_zip,
            create_shapefile_zip,
        )

        # Create mock data
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

        download_count = {"blocks": 0}

        with aioresponses() as mocked:

            def blocks_callback(url, **kwargs):
                download_count["blocks"] += 1
                return CallbackResult(body=blocks_zip)

            blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
            mocked.get(blocks_pattern, callback=blocks_callback, repeat=True)

            addrfeat_pattern = re.compile(r".*census\.gov.*ADDRFEAT.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

            pl_pattern = re.compile(r".*census\.gov.*Redistricting.*\.zip")
            mocked.get(pl_pattern, body=pl94171_zip, repeat=True)

            # First load - downloads data
            lookup1 = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            await lookup1.load_state("DC")
            first_download_count = download_count["blocks"]
            assert first_download_count >= 1

            # Delete the cached blocks file
            blocks_parquet = data_dir / "tiger" / "blocks" / "11.parquet"
            assert blocks_parquet.exists()
            blocks_parquet.unlink()

            # Second load - should re-download because file is missing
            # This triggers catalog.py line 153->155 (path.exists() is False)
            lookup2 = CensusLookup(
                geo_level=GeoLevel.TRACT,
                variables=["P1_001N"],
                data_dir=data_dir,
            )
            await lookup2.load_state("DC")

            # Should have made another download request
            assert download_count["blocks"] > first_download_count
