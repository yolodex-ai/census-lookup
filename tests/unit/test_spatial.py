"""Tests for spatial index and point-in-polygon operations."""

import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from census_lookup.core.spatial import SpatialIndex, MultiLevelSpatialIndex
from census_lookup.core.geoid import GeoLevel


class TestSpatialIndex:
    """Tests for SpatialIndex."""

    @pytest.fixture
    def spatial_index(self, sample_blocks):
        """Create a spatial index from sample blocks."""
        return SpatialIndex(sample_blocks, geoid_column="GEOID20")

    def test_lookup_point_in_first_block(self, spatial_index):
        """Test looking up a point in the first block."""
        point = Point(25, 25)  # Should be in first block
        geoid = spatial_index.lookup(point)

        assert geoid == "060371011001001"

    def test_lookup_point_in_second_block(self, spatial_index):
        """Test looking up a point in the second block."""
        point = Point(75, 25)  # Should be in second block
        geoid = spatial_index.lookup(point)

        assert geoid == "060371011001002"

    def test_lookup_point_in_third_block(self, spatial_index):
        """Test looking up a point in the third block."""
        point = Point(50, 75)  # Should be in third block
        geoid = spatial_index.lookup(point)

        assert geoid == "060371011002001"

    def test_lookup_point_outside_all_blocks(self, spatial_index):
        """Test looking up a point outside all blocks."""
        point = Point(200, 200)  # Outside all blocks
        geoid = spatial_index.lookup(point)

        assert geoid is None

    def test_lookup_coordinates(self, spatial_index):
        """Test looking up by lat/lon coordinates."""
        # Note: Shapely uses (x, y) = (lon, lat)
        geoid = spatial_index.lookup_coordinates(lat=25, lon=25)

        assert geoid == "060371011001001"

    def test_lookup_batch(self, spatial_index):
        """Test batch lookup with multiple points."""
        points = gpd.GeoSeries(
            [
                Point(25, 25),   # Block 1
                Point(75, 25),   # Block 2
                Point(50, 75),   # Block 3
                Point(200, 200), # Outside
            ],
            crs="EPSG:4269",
        )

        result = spatial_index.lookup_batch(points)

        assert len(result) == 4
        assert result.iloc[0]["GEOID"] == "060371011001001"
        assert result.iloc[1]["GEOID"] == "060371011001002"
        assert result.iloc[2]["GEOID"] == "060371011002001"
        assert pd.isna(result.iloc[3]["GEOID"])

    def test_lookup_dataframe(self, spatial_index):
        """Test looking up coordinates from a DataFrame."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "lat": [25, 75, 50],
            "lon": [25, 25, 75],
        })

        result = spatial_index.lookup_dataframe(df, lat_column="lat", lon_column="lon")

        assert "GEOID" in result.columns
        assert result.iloc[0]["GEOID"] == "060371011001001"

    def test_bounds_property(self, spatial_index):
        """Test bounds property returns bounding box."""
        bounds = spatial_index.bounds

        assert len(bounds) == 4
        assert bounds[0] == 0   # minx
        assert bounds[1] == 0   # miny
        assert bounds[2] == 100 # maxx
        assert bounds[3] == 100 # maxy

    def test_crs_property(self, spatial_index):
        """Test CRS property."""
        assert spatial_index.crs is not None


class TestMultiLevelSpatialIndex:
    """Tests for MultiLevelSpatialIndex."""

    @pytest.fixture
    def multi_index(self, sample_blocks):
        """Create a multi-level spatial index."""
        idx = MultiLevelSpatialIndex()
        idx.add_level(GeoLevel.BLOCK, sample_blocks, "GEOID20")
        return idx

    def test_has_level(self, multi_index):
        """Test checking for available levels."""
        assert multi_index.has_level(GeoLevel.BLOCK) is True
        assert multi_index.has_level(GeoLevel.TRACT) is False

    def test_lookup_at_block_level(self, multi_index):
        """Test lookup at block level."""
        point = Point(25, 25)
        geoid = multi_index.lookup(point, GeoLevel.BLOCK)

        assert geoid == "060371011001001"

    def test_lookup_at_tract_level_derived(self, multi_index):
        """Test lookup at tract level (derived from block)."""
        point = Point(25, 25)
        geoid = multi_index.lookup(point, GeoLevel.TRACT)

        assert geoid == "06037101100"

    def test_lookup_at_county_level_derived(self, multi_index):
        """Test lookup at county level (derived from block)."""
        point = Point(25, 25)
        geoid = multi_index.lookup(point, GeoLevel.COUNTY)

        assert geoid == "06037"

    def test_lookup_coordinates(self, multi_index):
        """Test coordinate lookup with level."""
        geoid = multi_index.lookup_coordinates(lat=25, lon=25, level=GeoLevel.TRACT)

        assert geoid == "06037101100"

    def test_lookup_all_levels(self, multi_index):
        """Test looking up all levels at once."""
        point = Point(25, 25)
        result = multi_index.lookup_all_levels(point)

        assert result[GeoLevel.BLOCK] == "060371011001001"
        assert result[GeoLevel.BLOCK_GROUP] == "060371011001"
        assert result[GeoLevel.TRACT] == "06037101100"
        assert result[GeoLevel.COUNTY] == "06037"
        assert result[GeoLevel.STATE] == "06"

    def test_available_levels(self, multi_index):
        """Test listing available levels."""
        levels = multi_index.available_levels

        assert GeoLevel.BLOCK in levels
