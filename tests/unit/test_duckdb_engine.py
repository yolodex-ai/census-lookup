"""Tests for DuckDB engine census data queries."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from census_lookup.core.geoid import GeoLevel
from census_lookup.data.duckdb_engine import DuckDBEngine


@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory with mock census parquet files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        census_dir = data_dir / "census" / "pl94171"
        census_dir.mkdir(parents=True)

        # Create mock census data at block level (15-digit GEOIDs)
        # State 11 (DC), County 001, Tract 010100
        # Create multiple blocks within the same tract to test aggregation
        mock_data = pd.DataFrame(
            {
                "GEOID": [
                    "110010101001001",  # Block 1 in tract 010100
                    "110010101001002",  # Block 2 in tract 010100
                    "110010101001003",  # Block 3 in tract 010100
                    "110010101002001",  # Block 1 in tract 010100, block group 2
                    "110010102001001",  # Block in different tract 010200
                ],
                "P1_001N": [100, 200, 300, 150, 500],  # Population
                "H1_001N": [40, 80, 120, 60, 200],  # Housing units
            }
        )
        mock_data.to_parquet(census_dir / "11.parquet", index=False)

        yield data_dir


class TestDuckDBEngineAggregation:
    """Tests for DuckDB census data aggregation at different geographic levels."""

    def test_join_census_data_block_level(self, temp_data_dir):
        """Test that block-level lookups return individual block data."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.join_census_data(
            geoids=["110010101001001"],
            variables=["P1_001N", "H1_001N"],
            geo_level=GeoLevel.BLOCK,
        )

        assert len(result) == 1
        assert result.iloc[0]["P1_001N"] == 100
        assert result.iloc[0]["H1_001N"] == 40
        engine.close()

    def test_join_census_data_tract_level_aggregation(self, temp_data_dir):
        """Test that tract-level lookups aggregate block data correctly."""
        engine = DuckDBEngine(temp_data_dir)

        # Tract 11001010100 contains blocks with populations: 100, 200, 300, 150 = 750
        result = engine.join_census_data(
            geoids=["11001010100"],  # 11-digit tract GEOID
            variables=["P1_001N", "H1_001N"],
            geo_level=GeoLevel.TRACT,
        )

        assert len(result) == 1
        # Sum of populations: 100 + 200 + 300 + 150 = 750
        assert result.iloc[0]["P1_001N"] == 750
        # Sum of housing units: 40 + 80 + 120 + 60 = 300
        assert result.iloc[0]["H1_001N"] == 300
        engine.close()

    def test_join_census_data_block_group_level_aggregation(self, temp_data_dir):
        """Test that block-group level lookups aggregate block data correctly."""
        engine = DuckDBEngine(temp_data_dir)

        # Block group 110010101001 contains blocks: 1001, 1002, 1003
        result = engine.join_census_data(
            geoids=["110010101001"],  # 12-digit block group GEOID
            variables=["P1_001N"],
            geo_level=GeoLevel.BLOCK_GROUP,
        )

        assert len(result) == 1
        # Sum of populations in block group 1: 100 + 200 + 300 = 600
        assert result.iloc[0]["P1_001N"] == 600
        engine.close()

    def test_join_census_data_county_level_aggregation(self, temp_data_dir):
        """Test that county-level lookups aggregate all blocks in county."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.join_census_data(
            geoids=["11001"],  # 5-digit county GEOID
            variables=["P1_001N", "H1_001N"],
            geo_level=GeoLevel.COUNTY,
        )

        assert len(result) == 1
        # Sum of all populations: 100 + 200 + 300 + 150 + 500 = 1250
        assert result.iloc[0]["P1_001N"] == 1250
        # Sum of all housing: 40 + 80 + 120 + 60 + 200 = 500
        assert result.iloc[0]["H1_001N"] == 500
        engine.close()

    def test_join_census_data_state_level_aggregation(self, temp_data_dir):
        """Test that state-level lookups aggregate all blocks in state."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.join_census_data(
            geoids=["11"],  # 2-digit state GEOID
            variables=["P1_001N"],
            geo_level=GeoLevel.STATE,
        )

        assert len(result) == 1
        # Sum of all populations: 1250
        assert result.iloc[0]["P1_001N"] == 1250
        engine.close()

    def test_join_census_data_multiple_geoids(self, temp_data_dir):
        """Test that multiple GEOIDs are handled correctly."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.join_census_data(
            geoids=["11001010100", "11001010200"],  # Two different tracts
            variables=["P1_001N"],
            geo_level=GeoLevel.TRACT,
        )

        assert len(result) == 2
        result_dict = result.set_index("GEOID")["P1_001N"].to_dict()
        assert result_dict["11001010100"] == 750  # 100 + 200 + 300 + 150
        assert result_dict["11001010200"] == 500  # Single block
        engine.close()

    def test_join_census_data_nonexistent_geoid(self, temp_data_dir):
        """Test that nonexistent GEOIDs return null values."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.join_census_data(
            geoids=["99999999999"],  # Nonexistent tract
            variables=["P1_001N"],
            geo_level=GeoLevel.TRACT,
        )

        assert len(result) == 1
        assert pd.isna(result.iloc[0]["P1_001N"])
        engine.close()


class TestGetVariablesForGeoid:
    """Tests for get_variables_for_geoid with auto-detection."""

    def test_auto_detect_tract_level(self, temp_data_dir):
        """Test that geo_level is auto-detected from GEOID length."""
        engine = DuckDBEngine(temp_data_dir)

        # Pass an 11-digit GEOID without specifying geo_level
        result = engine.get_variables_for_geoid(
            geoid="11001010100",  # 11 digits = tract
            variables=["P1_001N"],
            geo_level=None,  # Should auto-detect
        )

        # Should aggregate to tract level
        assert result["P1_001N"] == 750
        engine.close()

    def test_auto_detect_block_level(self, temp_data_dir):
        """Test auto-detection for block-level GEOID."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.get_variables_for_geoid(
            geoid="110010101001001",  # 15 digits = block
            variables=["P1_001N"],
            geo_level=None,
        )

        # Should return individual block data
        assert result["P1_001N"] == 100
        engine.close()

    def test_auto_detect_county_level(self, temp_data_dir):
        """Test auto-detection for county-level GEOID."""
        engine = DuckDBEngine(temp_data_dir)

        result = engine.get_variables_for_geoid(
            geoid="11001",  # 5 digits = county
            variables=["P1_001N"],
            geo_level=None,
        )

        # Should aggregate all blocks in county
        assert result["P1_001N"] == 1250
        engine.close()


class TestBatchLookup:
    """Tests for batch_lookup with aggregation."""

    def test_batch_lookup_tract_level(self, temp_data_dir):
        """Test batch lookup at tract level with aggregation."""
        engine = DuckDBEngine(temp_data_dir)

        input_df = pd.DataFrame(
            {
                "address": ["Address 1", "Address 2"],
                "tract_geoid": ["11001010100", "11001010200"],
            }
        )

        result = engine.batch_lookup(
            df=input_df,
            geoid_column="tract_geoid",
            variables=["P1_001N", "H1_001N"],
            geo_level=GeoLevel.TRACT,
        )

        assert len(result) == 2
        assert "P1_001N" in result.columns
        assert "H1_001N" in result.columns
        # Check that original columns are preserved
        assert "address" in result.columns

        # Find rows by address
        row1 = result[result["address"] == "Address 1"].iloc[0]
        row2 = result[result["address"] == "Address 2"].iloc[0]

        assert row1["P1_001N"] == 750
        assert row2["P1_001N"] == 500
        engine.close()
