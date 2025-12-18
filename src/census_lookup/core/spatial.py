"""Spatial index and point-in-polygon operations."""

from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


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
