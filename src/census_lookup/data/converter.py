"""Convert shapefiles to GeoParquet format."""

from pathlib import Path
from typing import Any, List, Literal, cast

import geopandas as gpd
import pandas as pd

# Note: pyarrow/geopandas supports zstd but type stubs may not include it
CompressionType = Literal["snappy", "gzip", "brotli", "zstd"] | None


class GeoParquetConverter:
    """
    Converts shapefiles to GeoParquet for efficient storage and loading.

    Benefits:
    - ~60% smaller file size
    - 2-4x faster reading
    - Column-selective reads
    - Spatial filtering support
    """

    def __init__(self, compression: CompressionType = "zstd"):
        """
        Initialize converter.

        Args:
            compression: Compression algorithm (zstd, snappy, gzip, or None)
        """
        self.compression: CompressionType = compression

    def convert_shapefile(
        self,
        shapefile_dir: Path,
        output_path: Path,
        columns: List[str],
    ) -> Path:
        """
        Convert a shapefile to GeoParquet.

        Args:
            shapefile_dir: Directory containing .shp file (from download extraction)
            output_path: Output parquet path
            columns: Columns to include (geometry always included)

        Returns:
            Path to output parquet file
        """
        # Find .shp file in extracted directory
        shp_files = list(shapefile_dir.glob("*.shp"))
        shapefile_path = shp_files[0]

        gdf = gpd.read_file(shapefile_path)

        # Always include geometry
        columns = list(set(columns) | {"geometry"})
        # Filter to existing columns
        existing_cols = [c for c in columns if c in gdf.columns]
        gdf = gdf[existing_cols]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Cast to Any for compression - zstd supported but not in stubs
        gdf.to_parquet(output_path, compression=cast(Any, self.compression))

        return output_path

    def convert_blocks(
        self,
        shapefile_dir: Path,
        output_path: Path,
    ) -> Path:
        """
        Convert block shapefile to GeoParquet with essential columns only.

        Essential columns for blocks:
        - GEOID20: Block GEOID (15-digit string)
        - STATEFP20: State FIPS
        - COUNTYFP20: County FIPS
        - TRACTCE20: Tract code
        - BLOCKCE20: Block code
        - geometry: Polygon geometry

        Raises:
            ValueError: If GEOID20 values are not valid 15-digit strings
        """
        columns = [
            "GEOID20",
            "STATEFP20",
            "COUNTYFP20",
            "TRACTCE20",
            "BLOCKCE20",
            "ALAND20",  # Land area
            "AWATER20",  # Water area
            "geometry",
        ]

        # Find .shp file in extracted directory
        shp_files = list(shapefile_dir.glob("*.shp"))
        shapefile_path = shp_files[0]

        gdf = gpd.read_file(shapefile_path)

        # Validate GEOID20 format
        invalid_geoids = gdf[~gdf["GEOID20"].str.match(r"^\d{15}$", na=False)]
        if not invalid_geoids.empty:
            geoid_col = invalid_geoids["GEOID20"]
            samples = cast(pd.Series, geoid_col).head(5).tolist()
            raise ValueError(
                f"Invalid GEOID20 values in block data (expected 15 digits): {samples}"
            )

        # Filter columns
        columns = list(set(columns) | {"geometry"})
        existing_cols = [c for c in columns if c in gdf.columns]
        gdf = gdf[existing_cols]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(output_path, compression=cast(Any, self.compression))

        return output_path

    def convert_address_features(
        self,
        shapefile_dir: Path,
        output_path: Path,
    ) -> Path:
        """
        Convert address feature shapefile to GeoParquet.

        Essential columns:
        - FULLNAME: Full street name
        - LFROMHN, LTOHN: Left side house number range
        - RFROMHN, RTOHN: Right side house number range
        - ZIPL, ZIPR: ZIP codes for each side
        - LINEARID: Feature ID
        - geometry: LineString geometry
        """
        columns = [
            "LINEARID",
            "FULLNAME",
            "LFROMHN",
            "LTOHN",
            "RFROMHN",
            "RTOHN",
            "ZIPL",
            "ZIPR",
            "PARITYL",
            "PARITYR",
            "geometry",
        ]
        return self.convert_shapefile(shapefile_dir, output_path, columns)

    def merge_county_files(
        self,
        county_files: List[Path],
        output_path: Path,
    ) -> Path:
        """
        Merge multiple county-level parquet files into a single state-level file.

        Args:
            county_files: List of county-level parquet files (must not be empty)
            output_path: Output state-level parquet file

        Returns:
            Path to merged output file
        """
        # Caller ensures county_files is non-empty
        gdfs = [gpd.read_parquet(path) for path in county_files]
        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(output_path, compression=cast(Any, self.compression))

        return output_path

    def convert_census_csv(
        self,
        csv_path: Path,
        output_path: Path,
        geoid_column: str = "GEO_ID",
    ) -> Path:
        """
        Convert Census CSV data to Parquet.

        Args:
            csv_path: Input CSV path
            output_path: Output parquet path
            geoid_column: Column containing GEOID

        Returns:
            Path to output parquet file
        """
        df = pd.read_csv(csv_path, dtype={geoid_column: str})

        # Clean up GEO_ID format (Census API returns "1000000US{GEOID}")
        df["GEOID"] = df[geoid_column].str.extract(r"(\d+)$")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, compression=self.compression)

        return output_path
