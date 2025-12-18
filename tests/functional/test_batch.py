"""Functional tests for batch geocoding.

Tests batch processing of multiple addresses through the public API.
"""

import pandas as pd
import pytest

from census_lookup import CensusLookup, GeoLevel


class TestBatchLookup:
    """User can geocode multiple addresses at once."""

    async def test_batch_geocoding(self, mock_census_http, isolated_data_dir):
        """Batch geocode returns DataFrame with all results."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Pre-load DC to avoid race condition in concurrent batch geocoding
        await lookup.load_state("DC")

        results = await lookup.geocode_batch([
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "100 Maryland Ave SW, Washington, DC",
        ])

        assert len(results) == 2
        assert "geoid" in results.columns
        assert "P1_001N" in results.columns

    async def test_batch_from_series(self, mock_census_http, isolated_data_dir):
        """Batch accepts pandas Series as input."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        # Pre-load DC to avoid race condition in concurrent batch geocoding
        await lookup.load_state("DC")

        addresses = pd.Series([
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "100 Maryland Ave SW, Washington, DC",
        ])

        results = await lookup.geocode_batch(addresses, progress=False)

        assert len(results) == 2
        assert "geoid" in results.columns

    async def test_batch_with_unmatched(self, mock_census_http, isolated_data_dir):
        """Batch handles unmatched addresses gracefully."""
        lookup = CensusLookup(
            geo_level=GeoLevel.TRACT,
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
        )

        results = await lookup.geocode_batch([
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "completely invalid address that won't match",
        ])

        assert len(results) == 2
        # At least one should be matched
        assert results["match_type"].isin(["interpolated", "exact"]).sum() >= 1
