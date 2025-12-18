"""Functional tests for error handling.

Tests how the library handles invalid inputs and edge cases through the public API.
"""

import pytest

from census_lookup import CensusLookup, GeoLevel


class TestInvalidStateErrors:
    """Test error handling for invalid state inputs."""

    async def test_invalid_state_fips_raises(self, mock_census_http, isolated_data_dir):
        """Invalid FIPS code raises ValueError."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        with pytest.raises(ValueError, match="Unknown state"):
            await lookup.load_state("99")

    async def test_invalid_state_abbrev_raises(self, mock_census_http, isolated_data_dir):
        """Invalid state abbreviation raises ValueError."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        with pytest.raises(ValueError, match="Unknown state"):
            await lookup.load_state("XX")

    async def test_invalid_state_name_raises(self, mock_census_http, isolated_data_dir):
        """Invalid state name raises ValueError."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        with pytest.raises(ValueError, match="Unknown state"):
            await lookup.load_state("NotAState")


class TestErrorHandling:
    """Errors are handled gracefully."""

    async def test_invalid_address(self, mock_census_http, isolated_data_dir):
        """Invalid address returns no_match result."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("this is not a valid address")

        assert not result.is_matched
        assert result.match_type in ["no_match", "no_state", "parse_error"]

    async def test_address_without_state(self, mock_census_http, isolated_data_dir):
        """Address without state returns no_state."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("123 Main Street")

        assert not result.is_matched
        assert result.match_type in ["no_state", "parse_error"]

    async def test_empty_address(self, mock_census_http, isolated_data_dir):
        """Empty address returns parse_error."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("")

        assert not result.is_matched
        assert result.match_type == "parse_error"

    async def test_whitespace_only_address(self, mock_census_http, isolated_data_dir):
        """Whitespace-only address returns parse_error."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        result = await lookup.geocode("   ")

        assert not result.is_matched
        assert result.match_type == "parse_error"
