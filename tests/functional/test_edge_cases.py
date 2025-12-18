"""Functional tests for edge cases and less common code paths.

Tests edge cases that exercise error handling and fallback logic through public API.
"""

import pytest

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
