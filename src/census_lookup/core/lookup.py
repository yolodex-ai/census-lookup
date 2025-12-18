"""Main CensusLookup class - the primary user interface."""

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from census_lookup.address.matcher import TIGERAddressMatcher
from census_lookup.address.parser import AddressParser, ParsedAddress
from census_lookup.census.acs import (
    ACS_VARIABLE_GROUPS,
    ACS_VARIABLES,
    get_acs_variables_for_group,
)
from census_lookup.census.variables import VARIABLES, get_variables_for_group
from census_lookup.core.geoid import GEOIDParser, GeoLevel
from census_lookup.core.spatial import SpatialIndex
from census_lookup.data.constants import normalize_state
from census_lookup.data.manager import DataManager


@dataclass
class LookupResult:
    """Result from a single address/coordinate lookup."""

    # Input
    input_address: Optional[str] = None
    parsed_address: Dict[str, Any] = field(default_factory=dict)

    # Geocoding results
    matched_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    match_type: str = "no_match"  # "exact", "interpolated", "no_match"
    match_score: float = 0.0  # 0.0 to 1.0

    # Geographic identifiers
    geoid: Optional[str] = None  # Full GEOID at requested level
    state_fips: Optional[str] = None
    county_fips: Optional[str] = None
    tract: Optional[str] = None
    block_group: Optional[str] = None
    block: Optional[str] = None

    # Census data (dynamic based on selected variables)
    census_data: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_matched(self) -> bool:
        """Check if the lookup was successful."""
        return self.geoid is not None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "input_address": self.input_address,
            "matched_address": self.matched_address,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "match_type": self.match_type,
            "match_score": self.match_score,
            "geoid": self.geoid,
            "state_fips": self.state_fips,
            "county_fips": self.county_fips,
            "tract": self.tract,
            "block_group": self.block_group,
            "block": self.block,
            **self.census_data,
        }

    def to_series(self) -> pd.Series:
        """Convert to pandas Series."""
        return pd.Series(self.to_dict())


class CensusLookup:
    """
    Main interface for census-lookup library.

    All methods are async for efficient concurrent operation.

    Example usage:
        >>> lookup = CensusLookup()
        >>> await lookup.load_state("CA")  # Lazy download if needed
        >>> result = await lookup.geocode("123 Main St, Los Angeles, CA 90012")
        >>> print(result.geoid, result.census_data)
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        geo_level: GeoLevel = GeoLevel.BLOCK,
        variables: Optional[List[str]] = None,
        variable_groups: Optional[List[str]] = None,
        acs_variables: Optional[List[str]] = None,
        acs_variable_groups: Optional[List[str]] = None,
    ):
        """
        Initialize CensusLookup.

        Args:
            data_dir: Directory for cached data. Defaults to ~/.census-lookup/
            geo_level: Default geographic level for lookups
            variables: PL 94-171 Census variables (e.g., ["P1_001N", "P2_001N"])
            variable_groups: PL 94-171 variable groups (e.g., ["population", "housing"])
            acs_variables: ACS variables (e.g., ["B19013_001E", "B15003_022E"])
            acs_variable_groups: ACS variable groups (e.g., ["income", "education"])

        Note:
            ACS data is only available at tract level and above. If you request
            ACS variables with geo_level=BLOCK or BLOCK_GROUP, the ACS data will
            be joined at tract level.
        """
        self.geo_level = geo_level

        # Initialize data manager
        self._data_manager = DataManager(data_dir=data_dir)

        # Determine variables to use
        self._variables = self._resolve_variables(variables, variable_groups)
        self._acs_variables = self._resolve_acs_variables(acs_variables, acs_variable_groups)

        # State-specific components (loaded lazily)
        self._loaded_states: Dict[str, Dict[str, Any]] = {}

        # Shared components
        self._parser = AddressParser()

    async def close(self):
        """Close all async sessions."""
        await self._data_manager.close()

    def _resolve_variables(
        self,
        variables: Optional[List[str]],
        variable_groups: Optional[List[str]],
    ) -> List[str]:
        """Resolve PL 94-171 variable list from variables and/or groups."""
        result = set()

        if variables:
            result.update(variables)

        if variable_groups:
            for group in variable_groups:
                result.update(get_variables_for_group(group))

        if not result:
            # Default to basic population
            result.add("P1_001N")

        return sorted(result)

    def _resolve_acs_variables(
        self,
        acs_variables: Optional[List[str]],
        acs_variable_groups: Optional[List[str]],
    ) -> List[str]:
        """Resolve ACS variable list from variables and/or groups."""
        result = set()

        if acs_variables:
            result.update(acs_variables)

        if acs_variable_groups:
            for group in acs_variable_groups:
                result.update(get_acs_variables_for_group(group))

        return sorted(result)

    async def load_state(self, state: str, force_download: bool = False) -> None:
        """
        Load data for a state (downloads if not cached).

        Args:
            state: State name, abbreviation, or FIPS code
            force_download: Re-download even if cached
        """
        state_fips = normalize_state(state)

        if state_fips in self._loaded_states and not force_download:
            return

        # Ensure data is downloaded
        await self._data_manager.ensure_state_data(state_fips, show_progress=True)

        # Also download ACS data if ACS variables are requested
        if self._acs_variables:
            await self._data_manager.ensure_acs_data(
                state_fips,
                variables=self._acs_variables,
                show_progress=True,
            )

        # Load blocks for spatial lookup
        blocks = await self._data_manager.get_blocks(state_fips)
        spatial_index = SpatialIndex(blocks, geoid_column="GEOID20")

        # Load address features for geocoding
        addr_features = await self._data_manager.get_address_features(state_fips)
        geocoder = TIGERAddressMatcher(addr_features)

        self._loaded_states[state_fips] = {
            "spatial_index": spatial_index,
            "geocoder": geocoder,
        }

    async def load_states(self, states: List[str]) -> None:
        """Load multiple states concurrently."""
        await asyncio.gather(*[self.load_state(state) for state in states])

    async def _ensure_state_loaded(self, state_fips: str) -> None:
        """Ensure a state is loaded, loading it if necessary."""
        if state_fips not in self._loaded_states:
            await self.load_state(state_fips)

    def _get_state_from_address(self, parsed: ParsedAddress) -> Optional[str]:
        """Extract state FIPS from parsed address."""
        if parsed.state:
            try:
                return normalize_state(parsed.state)
            except ValueError:
                pass
        return None

    async def geocode(
        self,
        address: str,
        geo_level: Optional[GeoLevel] = None,
    ) -> LookupResult:
        """
        Geocode a single address and return census data.

        Args:
            address: Full address string
            geo_level: Override default geographic level

        Returns:
            LookupResult with coordinates, GEOID, and census data
        """
        level = geo_level or self.geo_level

        # Parse address
        try:
            parsed = self._parser.parse(address)
        except Exception:
            return LookupResult(
                input_address=address,
                match_type="parse_error",
            )

        # Get state
        state_fips = self._get_state_from_address(parsed)
        if not state_fips:
            return LookupResult(
                input_address=address,
                parsed_address=parsed.to_dict(),
                match_type="no_state",
            )

        # Ensure state is loaded (state_fips is already validated by _get_state_from_address)
        await self._ensure_state_loaded(state_fips)

        state_data = self._loaded_states[state_fips]

        # Geocode
        geocode_result = state_data["geocoder"].geocode_parsed(parsed)

        if not geocode_result.is_matched:
            return LookupResult(
                input_address=address,
                parsed_address=parsed.to_dict(),
                match_type="no_match",
            )

        # Spatial lookup
        point = Point(geocode_result.longitude, geocode_result.latitude)
        block_geoid = state_data["spatial_index"].lookup(point)

        if not block_geoid:
            return LookupResult(
                input_address=address,
                parsed_address=parsed.to_dict(),
                latitude=geocode_result.latitude,
                longitude=geocode_result.longitude,
                matched_address=geocode_result.matched_address,
                match_type="no_block",
            )

        # Truncate GEOID to requested level
        geoid = block_geoid[: level.geoid_length]

        # Parse GEOID components
        components = GEOIDParser.parse(block_geoid)

        # Get census data (PL 94-171)
        census_data = self._data_manager.duckdb.get_variables_for_geoid(
            geoid,
            self._variables,
        )

        # Get ACS data if requested (at tract level)
        if self._acs_variables:
            tract_geoid = block_geoid[:11]  # Truncate to tract level
            acs_df = await self._data_manager.get_acs_data(
                components.state,
                self._acs_variables,
            )
            # Find matching tract
            acs_row = acs_df[acs_df["GEOID"] == tract_geoid]
            if not acs_row.empty:
                for var in self._acs_variables:
                    if var in acs_row.columns:
                        col = cast(pd.Series, acs_row[var])
                        raw_value: Any = col.iloc[0]
                        if bool(pd.notna(raw_value)):
                            census_data[var] = float(raw_value)
                        else:
                            census_data[var] = None

        return LookupResult(
            input_address=address,
            parsed_address=parsed.to_dict(),
            matched_address=geocode_result.matched_address,
            latitude=geocode_result.latitude,
            longitude=geocode_result.longitude,
            match_type=geocode_result.match_type,
            match_score=geocode_result.match_score,
            geoid=geoid,
            state_fips=components.state,
            county_fips=components.county_fips,
            tract=components.tract_geoid,
            block_group=components.block_group_geoid,
            block=block_geoid if level == GeoLevel.BLOCK else None,
            census_data=census_data,
        )

    async def geocode_batch(
        self,
        addresses: Union[List[str], pd.Series],
        geo_level: Optional[GeoLevel] = None,
        progress: bool = True,
    ) -> pd.DataFrame:
        """
        Geocode multiple addresses concurrently.

        Args:
            addresses: List or Series of address strings
            geo_level: Geographic level for results
            progress: Show progress bar

        Returns:
            DataFrame with original addresses and census data
        """
        if isinstance(addresses, pd.Series):
            addresses = addresses.tolist()

        # Geocode all addresses concurrently
        tasks = [self.geocode(address, geo_level) for address in addresses]

        if progress:
            from tqdm import tqdm

            results = []
            with tqdm(total=len(tasks), desc="Geocoding") as pbar:
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    results.append(result.to_dict())
                    pbar.update(1)
        else:
            results_list = await asyncio.gather(*tasks)
            results = [r.to_dict() for r in results_list]

        return pd.DataFrame(results)

    async def lookup_coordinates(
        self,
        lat: float,
        lon: float,
        geo_level: Optional[GeoLevel] = None,
    ) -> LookupResult:
        """
        Look up census data for a coordinate pair.

        Args:
            lat: Latitude (decimal degrees)
            lon: Longitude (decimal degrees)
            geo_level: Override default geographic level

        Returns:
            LookupResult with GEOID and census data
        """
        level = geo_level or self.geo_level

        # Determine state from coordinates (rough bounding box check)
        # For now, require state to be loaded first
        point = Point(lon, lat)

        # Try each loaded state
        block_geoid = None
        for _, state_data in self._loaded_states.items():
            geoid = state_data["spatial_index"].lookup(point)
            if geoid:
                block_geoid = geoid
                break

        if not block_geoid:
            return LookupResult(
                latitude=lat,
                longitude=lon,
                match_type="no_block",
            )

        # Truncate to requested level
        geoid = block_geoid[: level.geoid_length]
        components = GEOIDParser.parse(block_geoid)

        # Get census data (PL 94-171)
        census_data = self._data_manager.duckdb.get_variables_for_geoid(
            geoid,
            self._variables,
        )

        # Get ACS data if requested (at tract level)
        if self._acs_variables:
            tract_geoid = block_geoid[:11]  # Truncate to tract level
            acs_df = await self._data_manager.get_acs_data(
                components.state,
                self._acs_variables,
            )
            # Find matching tract - may be missing if ACS data is incomplete
            acs_row = acs_df[acs_df["GEOID"] == tract_geoid]
            if not acs_row.empty:
                for var in self._acs_variables:
                    # Variable may be missing if API didn't return it
                    if var in acs_row.columns:
                        col = cast(pd.Series, acs_row[var])
                        raw_value: Any = col.iloc[0]
                        if bool(pd.notna(raw_value)):
                            census_data[var] = float(raw_value)
                        else:
                            census_data[var] = None

        return LookupResult(
            latitude=lat,
            longitude=lon,
            match_type="coordinates",
            match_score=1.0,
            geoid=geoid,
            state_fips=components.state,
            county_fips=components.county_fips,
            tract=components.tract_geoid,
            block_group=components.block_group_geoid,
            block=block_geoid if level == GeoLevel.BLOCK else None,
            census_data=census_data,
        )

    async def lookup_coordinates_batch(
        self,
        df: pd.DataFrame,
        lat_column: str = "latitude",
        lon_column: str = "longitude",
        geo_level: Optional[GeoLevel] = None,
    ) -> pd.DataFrame:
        """
        Batch lookup for coordinates in a DataFrame.

        Args:
            df: DataFrame with lat/lon columns
            lat_column: Name of latitude column
            lon_column: Name of longitude column
            geo_level: Geographic level for results

        Returns:
            DataFrame with GEOID and census data columns added
        """
        level = geo_level or self.geo_level

        # Create GeoSeries of points
        point_list: list[Point | None] = [
            Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None
            for lat, lon in zip(df[lat_column], df[lon_column])
        ]
        points = gpd.GeoSeries(cast(Any, point_list), crs="EPSG:4269")  # NAD 83

        # Spatial join with each loaded state
        result = df.copy()
        result["_geoid"] = None

        for _, state_data in self._loaded_states.items():
            geoids = state_data["spatial_index"].lookup_batch(points)
            # Fill in where we found matches
            mask = geoids["GEOID"].notna() & result["_geoid"].isna()
            result.loc[mask, "_geoid"] = geoids.loc[mask, "GEOID"]

        # Truncate to level
        result["GEOID"] = result["_geoid"].apply(lambda x: x[: level.geoid_length] if x else None)
        result = result.drop(columns=["_geoid"])

        # Join census data
        geoids = result["GEOID"].dropna().unique().tolist()
        if geoids:
            census_df = self._data_manager.duckdb.join_census_data(
                geoids,
                self._variables,
                level,
            )
            result = result.merge(census_df, on="GEOID", how="left")

        return result

    @property
    def loaded_states(self) -> List[str]:
        """List of currently loaded states."""
        return list(self._loaded_states.keys())

    @property
    def variables(self) -> List[str]:
        """List of selected census variables."""
        return self._variables

    @property
    def available_variables(self) -> Dict[str, str]:
        """Dictionary of all available census variables and descriptions."""
        return VARIABLES.copy()

    def set_variables(self, variables: List[str]) -> None:
        """Update the list of census variables to retrieve."""
        self._variables = sorted(set(variables))

    def add_variable_group(self, group: str) -> None:
        """Add PL 94-171 variables from a group to the current selection."""
        new_vars = get_variables_for_group(group)
        self._variables = sorted(set(self._variables) | set(new_vars))

    # ==========================================================================
    # ACS (American Community Survey) Support
    # ==========================================================================

    @property
    def acs_variables(self) -> List[str]:
        """List of selected ACS variables."""
        return self._acs_variables

    @property
    def available_acs_variables(self) -> Dict[str, str]:
        """Dictionary of all available ACS variables and descriptions."""
        return ACS_VARIABLES.copy()

    @property
    def available_acs_variable_groups(self) -> Dict[str, List[str]]:
        """Dictionary of ACS variable groups."""
        return ACS_VARIABLE_GROUPS.copy()

    def set_acs_variables(self, variables: List[str]) -> None:
        """Update the list of ACS variables to retrieve."""
        self._acs_variables = sorted(set(variables))

    def add_acs_variable_group(self, group: str) -> None:
        """Add ACS variables from a group to the current selection."""
        new_vars = get_acs_variables_for_group(group)
        self._acs_variables = sorted(set(self._acs_variables) | set(new_vars))

    def clear_acs_variables(self) -> None:
        """Clear all ACS variables."""
        self._acs_variables = []
