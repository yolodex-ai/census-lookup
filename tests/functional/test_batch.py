"""Functional tests for batch geocoding.

Tests batch processing of multiple addresses through the public API.
"""

import pandas as pd

from census_lookup import CensusLookup


class TestBatchLookup:
    """User can geocode multiple addresses at once."""

    async def test_batch_geocoding(self, mock_census_http, isolated_data_dir):
        """Batch geocode returns DataFrame with flattened results."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
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

    async def test_batch_from_series(self, mock_census_http, isolated_data_dir):
        """Batch accepts pandas Series as input."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
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

    async def test_batch_with_unmatched(self, mock_census_http, isolated_data_dir):
        """Batch handles unmatched addresses gracefully."""
        lookup = CensusLookup(
            variables=["P1_001N"],
            data_dir=isolated_data_dir,
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
