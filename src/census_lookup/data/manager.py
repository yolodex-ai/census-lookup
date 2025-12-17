"""Data manager for orchestrating downloads, caching, and loading."""

import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from census_lookup.core.geoid import GeoLevel
from census_lookup.data.catalog import DataCatalog, DatasetInfo
from census_lookup.data.constants import FIPS_STATES, TIGER_URLS, normalize_state
from census_lookup.data.converter import GeoParquetConverter
from census_lookup.data.downloader import CensusDataDownloader, TIGERDownloader
from census_lookup.data.duckdb_engine import DuckDBEngine


class DataNotAvailableError(Exception):
    """Required data not downloaded."""

    def __init__(self, state: str, data_type: str):
        self.state = state
        self.data_type = data_type
        super().__init__(
            f"Data not available for {state} ({data_type}). "
            f"Run `census-lookup download {state}` to download."
        )


class DataManager:
    """
    Orchestrates data downloads, conversion, and caching.

    Directory structure:
    ~/.census-lookup/
    ├── catalog.json           # Tracks downloaded data
    ├── tiger/
    │   ├── addrfeat/          # Address range features
    │   │   ├── 06/            # State FIPS
    │   │   │   ├── 06001.parquet  # County-level files
    │   │   │   └── ...
    │   │   └── ...
    │   └── blocks/
    │       ├── 06.parquet     # State-level block files
    │       └── ...
    ├── census/
    │   ├── pl94171/
    │   │   ├── 06.parquet     # State-level census data
    │   │   └── ...
    │   └── variables.json     # Variable definitions
    └── temp/                   # Temporary download directory
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        auto_download: bool = True,
    ):
        """
        Initialize DataManager.

        Args:
            data_dir: Base directory for data. Defaults to ~/.census-lookup/
            auto_download: Whether to automatically download missing data
        """
        self.data_dir = data_dir or Path.home() / ".census-lookup"
        self.auto_download = auto_download

        # Create subdirectories
        self.tiger_dir = self.data_dir / "tiger"
        self.census_dir = self.data_dir / "census"
        self.temp_dir = self.data_dir / "temp"

        for d in [self.tiger_dir, self.census_dir, self.temp_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.catalog = DataCatalog(self.data_dir / "catalog.json")
        self.downloader = TIGERDownloader()
        self.census_downloader = CensusDataDownloader()
        self.converter = GeoParquetConverter()

        # DuckDB engine (lazy initialization)
        self._duckdb: Optional[DuckDBEngine] = None

    @property
    def duckdb(self) -> DuckDBEngine:
        """Get or create DuckDB engine."""
        if self._duckdb is None:
            self._duckdb = DuckDBEngine(self.data_dir)
        return self._duckdb

    def ensure_state_data(
        self,
        state: str,
        data_types: Optional[Set[str]] = None,
        show_progress: bool = True,
    ) -> None:
        """
        Ensure all required data for a state is downloaded and converted.

        Args:
            state: State name, abbreviation, or FIPS code
            data_types: Types of data to ensure. Defaults to {"blocks", "addrfeat", "pl94171"}
            show_progress: Show progress indicators
        """
        state_fips = normalize_state(state)

        if data_types is None:
            data_types = {"blocks", "addrfeat", "pl94171"}

        if "blocks" in data_types:
            self._ensure_blocks(state_fips, show_progress)

        if "addrfeat" in data_types:
            self._ensure_address_features(state_fips, show_progress)

        if "pl94171" in data_types:
            self._ensure_census_data(state_fips, show_progress)

    def _ensure_blocks(self, state_fips: str, show_progress: bool = True) -> None:
        """Ensure block data is available for a state."""
        if self.catalog.is_available("blocks", state_fips):
            return

        if show_progress:
            print(f"Downloading block data for {FIPS_STATES.get(state_fips, state_fips)}...")

        # Download shapefile
        temp_extract = self.downloader.download_blocks(state_fips, self.temp_dir)

        # Convert to parquet
        output_path = self.tiger_dir / "blocks" / f"{state_fips}.parquet"
        self.converter.convert_blocks(temp_extract, output_path)

        # Register in catalog
        info = DatasetInfo.create(
            dataset_type="blocks",
            state_fips=state_fips,
            file_path=output_path,
            source_url=TIGER_URLS["blocks"].format(state_fips=state_fips),
        )
        self.catalog.register(info)

        # Clean up temp
        shutil.rmtree(temp_extract, ignore_errors=True)

    def _ensure_address_features(self, state_fips: str, show_progress: bool = True) -> None:
        """Ensure address feature data is available for a state."""
        if self.catalog.is_available("addrfeat", state_fips):
            return

        if show_progress:
            print(f"Downloading address features for {FIPS_STATES.get(state_fips, state_fips)}...")

        # Get list of counties for this state
        county_fips_list = self._get_county_fips_list(state_fips)

        # Download each county
        county_files = self.downloader.download_address_features_for_state(
            state_fips,
            county_fips_list,
            self.temp_dir,
            show_progress=show_progress,
        )

        # Convert each to parquet
        parquet_files = []
        for shp_dir in county_files:
            county_fips = shp_dir.name.split("_")[2]  # Extract from tl_2020_XXXXX_addrfeat
            output_path = self.tiger_dir / "addrfeat" / state_fips / f"{county_fips}.parquet"
            self.converter.convert_address_features(shp_dir, output_path)
            parquet_files.append(output_path)

        # Merge into single state file
        state_output = self.tiger_dir / "addrfeat" / f"{state_fips}.parquet"
        if parquet_files:
            self.converter.merge_county_files(parquet_files, state_output)

            # Register in catalog
            info = DatasetInfo.create(
                dataset_type="addrfeat",
                state_fips=state_fips,
                file_path=state_output,
                source_url=TIGER_URLS["addrfeat"].format(county_fips="*"),
            )
            self.catalog.register(info)

        # Clean up temp
        for shp_dir in county_files:
            shutil.rmtree(shp_dir, ignore_errors=True)

    def _ensure_census_data(self, state_fips: str, show_progress: bool = True) -> None:
        """Ensure PL 94-171 census data is available for a state."""
        if self.catalog.is_available("pl94171", state_fips):
            return

        if show_progress:
            print(f"Downloading census data for {FIPS_STATES.get(state_fips, state_fips)}...")

        # Download via Census API
        from census_lookup.census.variables import DEFAULT_VARIABLES

        csv_path = self.temp_dir / f"pl94171_{state_fips}.csv"
        self.census_downloader.download_pl94171_for_state(
            state_fips,
            variables=DEFAULT_VARIABLES,
            geo_level="block",
            dest_path=csv_path,
        )

        # Convert to parquet
        output_path = self.census_dir / "pl94171" / f"{state_fips}.parquet"
        self.converter.convert_census_csv(csv_path, output_path)

        # Register in catalog
        info = DatasetInfo.create(
            dataset_type="pl94171",
            state_fips=state_fips,
            file_path=output_path,
            source_url="https://api.census.gov/data/2020/dec/pl",
        )
        self.catalog.register(info)

        # Clean up
        csv_path.unlink(missing_ok=True)

    def _get_county_fips_list(self, state_fips: str) -> List[str]:
        """
        Get list of county FIPS codes for a state.

        Uses static county data from constants module.
        """
        from census_lookup.data.constants import get_counties_for_state

        counties = get_counties_for_state(state_fips)
        # Return full 5-digit county FIPS (state + county)
        return [f"{state_fips}{county}" for county in counties]

    def get_blocks(self, state_fips: str) -> gpd.GeoDataFrame:
        """
        Load block polygons for a state.

        Args:
            state_fips: 2-digit state FIPS code

        Returns:
            GeoDataFrame with block polygons
        """
        state_fips = normalize_state(state_fips)

        if self.auto_download and not self.catalog.is_available("blocks", state_fips):
            self._ensure_blocks(state_fips)

        path = self.catalog.get_path("blocks", state_fips)
        if not path:
            raise DataNotAvailableError(state_fips, "blocks")

        return gpd.read_parquet(path)

    def get_address_features(self, state_fips: str) -> gpd.GeoDataFrame:
        """
        Load address features for a state.

        Args:
            state_fips: 2-digit state FIPS code

        Returns:
            GeoDataFrame with address range features
        """
        state_fips = normalize_state(state_fips)

        if self.auto_download and not self.catalog.is_available("addrfeat", state_fips):
            self._ensure_address_features(state_fips)

        path = self.catalog.get_path("addrfeat", state_fips)
        if not path:
            raise DataNotAvailableError(state_fips, "addrfeat")

        return gpd.read_parquet(path)

    def get_census_data(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: GeoLevel = GeoLevel.BLOCK,
    ) -> pd.DataFrame:
        """
        Load census data for specified variables.

        Args:
            state_fips: 2-digit state FIPS code
            variables: Census variable codes
            geo_level: Geographic level

        Returns:
            DataFrame with GEOID and requested variables
        """
        state_fips = normalize_state(state_fips)

        if self.auto_download and not self.catalog.is_available("pl94171", state_fips):
            self._ensure_census_data(state_fips)

        path = self.catalog.get_path("pl94171", state_fips)
        if not path:
            raise DataNotAvailableError(state_fips, "pl94171")

        # Use DuckDB for efficient querying
        geoid_length = geo_level.geoid_length
        var_list = ", ".join(variables)

        sql = f"""
        SELECT LEFT(GEOID, {geoid_length}) as GEOID, {var_list}
        FROM read_parquet('{path}')
        GROUP BY LEFT(GEOID, {geoid_length})
        """

        if geo_level == GeoLevel.BLOCK:
            # No aggregation needed for block level
            sql = f"SELECT GEOID, {var_list} FROM read_parquet('{path}')"

        return self.duckdb.query(sql)

    def clear_cache(self, state: Optional[str] = None) -> None:
        """
        Clear cached data.

        Args:
            state: State to clear (None for all states)
        """
        if state:
            state_fips = normalize_state(state)

            # Remove files
            for dataset_type in ["blocks", "addrfeat", "pl94171"]:
                path = self.catalog.get_path(dataset_type, state_fips)
                if path and path.exists():
                    path.unlink()
                self.catalog.unregister(dataset_type, state_fips)
        else:
            # Clear all
            shutil.rmtree(self.tiger_dir, ignore_errors=True)
            shutil.rmtree(self.census_dir, ignore_errors=True)
            self.catalog.clear()

            # Recreate directories
            self.tiger_dir.mkdir(parents=True, exist_ok=True)
            self.census_dir.mkdir(parents=True, exist_ok=True)

    def disk_usage(self) -> Dict[str, int]:
        """
        Get disk usage by category.

        Returns:
            Dictionary with category names and sizes in bytes
        """
        usage = {"total": 0}

        for category in ["tiger/blocks", "tiger/addrfeat", "census/pl94171"]:
            path = self.data_dir / category
            if path.exists():
                size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
                usage[category] = size
                usage["total"] += size

        return usage

    def list_available_states(self, dataset_type: str = "blocks") -> List[str]:
        """List states with available data."""
        return self.catalog.list_states(dataset_type)
