"""Convert shapefiles to GeoParquet format."""

from pathlib import Path
from typing import List, Optional

import geopandas as gpd
import pandas as pd


class GeoParquetConverter:
    """
    Converts shapefiles to GeoParquet for efficient storage and loading.

    Benefits:
    - ~60% smaller file size
    - 2-4x faster reading
    - Column-selective reads
    - Spatial filtering support
    """

    def __init__(self, compression: str = "zstd"):
        """
        Initialize converter.

        Args:
            compression: Compression algorithm (zstd, snappy, gzip, or None)
        """
        self.compression = compression

    def convert_shapefile(
        self,
        shapefile_path: Path,
        output_path: Path,
        columns: Optional[List[str]] = None,
    ) -> Path:
        """
        Convert a shapefile to GeoParquet.

        Args:
            shapefile_path: Input shapefile path (.shp or directory containing .shp)
            output_path: Output parquet path
            columns: Columns to include (None for all)

        Returns:
            Path to output parquet file
        """
        # Handle both .shp file and directory containing .shp
        if shapefile_path.is_dir():
            shp_files = list(shapefile_path.glob("*.shp"))
            if not shp_files:
                raise FileNotFoundError(f"No .shp file found in {shapefile_path}")
            shapefile_path = shp_files[0]

        gdf = gpd.read_file(shapefile_path)

        if columns:
            # Always include geometry
            columns = list(set(columns) | {"geometry"})
            # Filter to existing columns
            existing_cols = [c for c in columns if c in gdf.columns]
            gdf = gdf[existing_cols]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(output_path, compression=self.compression)

        return output_path

    def convert_blocks(
        self,
        shapefile_path: Path,
        output_path: Path,
    ) -> Path:
        """
        Convert block shapefile to GeoParquet with essential columns only.

        Essential columns for blocks:
        - GEOID20: Block GEOID
        - STATEFP20: State FIPS
        - COUNTYFP20: County FIPS
        - TRACTCE20: Tract code
        - BLOCKCE20: Block code
        - geometry: Polygon geometry
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
        return self.convert_shapefile(shapefile_path, output_path, columns)

    def convert_address_features(
        self,
        shapefile_path: Path,
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
        return self.convert_shapefile(shapefile_path, output_path, columns)

    def merge_county_files(
        self,
        county_files: List[Path],
        output_path: Path,
    ) -> Path:
        """
        Merge multiple county-level parquet files into a single state-level file.

        Args:
            county_files: List of county-level parquet files
            output_path: Output state-level parquet file

        Returns:
            Path to merged output file
        """
        if not county_files:
            raise ValueError("No county files provided")

        gdfs = []
        for path in county_files:
            if path.suffix == ".parquet":
                gdf = gpd.read_parquet(path)
            else:
                gdf = gpd.read_file(path)
            gdfs.append(gdf)

        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))

        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(output_path, compression=self.compression)

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
        if geoid_column in df.columns:
            df["GEOID"] = df[geoid_column].str.extract(r"(\d+)$")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, compression=self.compression)

        return output_path
