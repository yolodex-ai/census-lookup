"""Spatial index and point-in-polygon operations."""

from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from census_lookup.core.geoid import GeoLevel


class SpatialIndex:
    """
    Efficient spatial index for point-in-polygon lookups.

    Uses GeoPandas sindex (backed by rtree) for ~17x speedup
    over naive iteration.
    """

    def __init__(
        self,
        polygons: gpd.GeoDataFrame,
        geoid_column: str = "GEOID20",
    ):
        """
        Initialize spatial index from polygon GeoDataFrame.

        Args:
            polygons: GeoDataFrame with polygon geometries
            geoid_column: Column name containing GEOID
        """
        self._polygons = polygons
        self._geoid_col = geoid_column

        # Ensure spatial index is built
        if not self._polygons.has_sindex:
            _ = self._polygons.sindex

    def lookup(self, point: Point) -> Optional[str]:
        """
        Find GEOID containing a point.

        Uses two-phase approach:
        1. Query spatial index for candidate polygons (bbox intersection)
        2. Exact point-in-polygon test on candidates

        Args:
            point: Shapely Point object

        Returns:
            GEOID string if found, None otherwise
        """
        # Get candidate indices from spatial index
        candidates = list(self._polygons.sindex.query(point, predicate="intersects"))

        if len(candidates) == 0:
            return None

        # For census blocks, should typically be exactly one match
        # Use covers() instead of contains() to include boundary points
        # If multiple match (point on boundary), return smallest by area
        matches = []
        for idx in candidates:
            row = self._polygons.iloc[idx]
            geom = row.geometry
            # covers() returns True if point is inside OR on boundary
            if geom.covers(point):
                matches.append((row[self._geoid_col], geom.area))

        if not matches:
            return None

        # If multiple matches (boundary point), prefer smaller area block
        # This typically gives more specific result
        matches.sort(key=lambda x: x[1])
        return matches[0][0]

    def lookup_coordinates(
        self,
        lat: float,
        lon: float,
    ) -> Optional[str]:
        """
        Look up GEOID for lat/lon coordinates.

        Args:
            lat: Latitude (decimal degrees)
            lon: Longitude (decimal degrees)

        Returns:
            GEOID string if found, None otherwise
        """
        point = Point(lon, lat)  # Note: Shapely uses (x, y) = (lon, lat)
        return self.lookup(point)

    def lookup_batch(
        self,
        points: gpd.GeoSeries,
    ) -> pd.DataFrame:
        """
        Batch lookup using spatial join.

        Much faster than iterating for large point sets.
        Uses gpd.sjoin with "within" predicate.

        Args:
            points: GeoSeries of Point geometries

        Returns:
            DataFrame with GEOID for each input point
        """
        # Create GeoDataFrame from points
        points_gdf = gpd.GeoDataFrame(
            {"_idx": range(len(points))},
            geometry=points,
            crs=self._polygons.crs,
        )

        # Spatial join
        result = gpd.sjoin(
            points_gdf,
            self._polygons[[self._geoid_col, "geometry"]],
            how="left",
            predicate="within",
        )

        # Handle duplicates (shouldn't happen with census blocks, but be safe)
        result = result.drop_duplicates(subset="_idx", keep="first")

        # Ensure we have all original points
        result = result.set_index("_idx").reindex(range(len(points)))

        return result[[self._geoid_col]].rename(columns={self._geoid_col: "GEOID"})

    def lookup_dataframe(
        self,
        df: pd.DataFrame,
        lat_column: str,
        lon_column: str,
    ) -> pd.DataFrame:
        """
        Look up GEOIDs for coordinates in a DataFrame.

        Args:
            df: DataFrame with lat/lon columns
            lat_column: Name of latitude column
            lon_column: Name of longitude column

        Returns:
            Original DataFrame with GEOID column added
        """
        # Create points
        points = gpd.GeoSeries(
            [
                Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None
                for lat, lon in zip(df[lat_column], df[lon_column])
            ],
            crs=self._polygons.crs,
        )

        # Lookup
        geoids = self.lookup_batch(points)

        # Add to DataFrame
        result = df.copy()
        result["GEOID"] = geoids["GEOID"].values

        return result

    @property
    def bounds(self) -> tuple:
        """Get bounding box of indexed polygons."""
        return self._polygons.total_bounds

    @property
    def crs(self):
        """Get coordinate reference system."""
        return self._polygons.crs


class MultiLevelSpatialIndex:
    """
    Spatial index supporting multiple geographic levels.

    Maintains separate indexes for block, block_group, tract, county
    for efficient lookups at any level.

    Note: For most use cases, just using the block-level index and
    truncating the GEOID is sufficient and more memory-efficient.
    """

    def __init__(self):
        self._indexes: Dict[GeoLevel, SpatialIndex] = {}

    def add_level(
        self,
        level: GeoLevel,
        polygons: gpd.GeoDataFrame,
        geoid_column: str = "GEOID20",
    ) -> None:
        """
        Add a geographic level to the index.

        Args:
            level: Geographic level
            polygons: GeoDataFrame with polygons for this level
            geoid_column: Column containing GEOID
        """
        self._indexes[level] = SpatialIndex(polygons, geoid_column)

    def has_level(self, level: GeoLevel) -> bool:
        """Check if a level is indexed."""
        return level in self._indexes

    def lookup(
        self,
        point: Point,
        level: GeoLevel,
    ) -> Optional[str]:
        """
        Look up GEOID at specified level.

        Args:
            point: Shapely Point object
            level: Geographic level to look up

        Returns:
            GEOID at requested level, or None
        """
        if level not in self._indexes:
            # Try to derive from finer level
            if GeoLevel.BLOCK in self._indexes:
                block_geoid = self._indexes[GeoLevel.BLOCK].lookup(point)
                if block_geoid:
                    return block_geoid[: level.geoid_length]
            return None

        return self._indexes[level].lookup(point)

    def lookup_coordinates(
        self,
        lat: float,
        lon: float,
        level: GeoLevel = GeoLevel.BLOCK,
    ) -> Optional[str]:
        """
        Look up GEOID for coordinates at specified level.

        Args:
            lat: Latitude
            lon: Longitude
            level: Geographic level

        Returns:
            GEOID at requested level
        """
        point = Point(lon, lat)
        return self.lookup(point, level)

    def lookup_all_levels(
        self,
        point: Point,
    ) -> Dict[GeoLevel, Optional[str]]:
        """
        Look up GEOIDs at all available levels.

        Args:
            point: Shapely Point object

        Returns:
            Dictionary mapping level to GEOID
        """
        results = {}

        # If we have block level, derive others from it
        if GeoLevel.BLOCK in self._indexes:
            block_geoid = self._indexes[GeoLevel.BLOCK].lookup(point)
            if block_geoid:
                results[GeoLevel.BLOCK] = block_geoid
                results[GeoLevel.BLOCK_GROUP] = block_geoid[:12]
                results[GeoLevel.TRACT] = block_geoid[:11]
                results[GeoLevel.COUNTY] = block_geoid[:5]
                results[GeoLevel.STATE] = block_geoid[:2]
                return results

        # Otherwise, look up each level individually
        for level, index in self._indexes.items():
            results[level] = index.lookup(point)

        return results

    @property
    def available_levels(self) -> List[GeoLevel]:
        """List of indexed geographic levels."""
        return list(self._indexes.keys())
