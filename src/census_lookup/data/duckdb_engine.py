"""DuckDB-based data engine for census queries and joins."""

from pathlib import Path
from typing import Dict, List, Optional, Union

import duckdb
import pandas as pd

from census_lookup.core.geoid import GeoLevel


class DuckDBEngine:
    """
    DuckDB-based data engine for census queries.

    Handles:
    - Querying GeoParquet/Parquet files directly
    - Joining census data to GEOIDs
    - Aggregating data to higher geographic levels
    - Efficient batch operations
    """

    def __init__(self, data_dir: Path):
        """
        Initialize DuckDB engine.

        Args:
            data_dir: Base directory containing census data files
        """
        self.data_dir = data_dir
        self.conn = duckdb.connect()
        self._setup_extensions()

    def _setup_extensions(self) -> None:
        """Install and load required DuckDB extensions."""
        # Spatial extension for geometry support
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def __enter__(self) -> "DuckDBEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries

        Returns:
            DataFrame with query results
        """
        if params:
            result = self.conn.execute(sql, params)
        else:
            result = self.conn.execute(sql)
        return result.fetchdf()

    def get_census_parquet_path(self, state_fips: str) -> Path:
        """Get the path to census data parquet file for a state."""
        return self.data_dir / "census" / "pl94171" / f"{state_fips}.parquet"

    def join_census_data(
        self,
        geoids: Union[List[str], pd.Series],
        variables: List[str],
        geo_level: GeoLevel = GeoLevel.BLOCK,
    ) -> pd.DataFrame:
        """
        Join census variables to a list of GEOIDs.

        Args:
            geoids: List or Series of GEOIDs
            variables: Census variable codes to retrieve
            geo_level: Geographic level of the GEOIDs

        Returns:
            DataFrame with GEOID and requested variables
        """
        if isinstance(geoids, pd.Series):
            geoids = geoids.tolist()

        if not geoids:
            return pd.DataFrame(columns=["GEOID"] + variables)

        # Determine which states we need
        state_fips_set = set(g[:2] for g in geoids if g and len(g) >= 2)

        # Build query for each state and union
        parquet_paths = []
        for state_fips in state_fips_set:
            path = self.get_census_parquet_path(state_fips)
            if path.exists():
                parquet_paths.append(str(path))

        if not parquet_paths:
            # No data available, return empty with GEOIDs
            return pd.DataFrame({"GEOID": geoids, **{v: None for v in variables}})

        # Register GEOIDs as a temporary table
        geoid_df = pd.DataFrame({"GEOID": geoids})
        self.conn.register("input_geoids", geoid_df)

        # Handle GEOID truncation for higher levels
        geoid_length = geo_level.geoid_length

        # Build parquet glob pattern
        parquet_glob = f"read_parquet([{', '.join(repr(p) for p in parquet_paths)}])"

        # Build the variable list for SELECT (from the subquery result 'c')
        var_list = ", ".join([f"c.{v}" for v in variables])

        # For higher geographic levels, we need to aggregate (SUM) the block data
        # The census parquet files store data at block level, so we GROUP BY
        # the truncated GEOID to get tract/county/state totals
        agg_vars = ", ".join([f"SUM({v}) as {v}" for v in variables])

        sql = f"""
        SELECT
            i.GEOID,
            {var_list}
        FROM input_geoids i
        LEFT JOIN (
            SELECT LEFT(GEOID, {geoid_length}) as GEOID, {agg_vars}
            FROM {parquet_glob}
            GROUP BY LEFT(GEOID, {geoid_length})
        ) c ON i.GEOID = c.GEOID
        """

        result = self.query(sql)
        self.conn.unregister("input_geoids")

        return result

    def get_variables_for_geoid(
        self,
        geoid: str,
        variables: List[str],
        geo_level: Optional[GeoLevel] = None,
    ) -> Dict[str, Optional[float]]:
        """
        Get census variables for a single GEOID.

        Args:
            geoid: Geographic identifier
            variables: Variables to retrieve
            geo_level: Geographic level (auto-detected from GEOID length if not provided)

        Returns:
            Dictionary of variable values
        """
        # Auto-detect geo_level from GEOID length if not provided
        if geo_level is None:
            geo_level = GeoLevel.from_geoid_length(len(geoid))

        result = self.join_census_data([geoid], variables, geo_level)

        if result.empty:
            return {v: None for v in variables}

        row = result.iloc[0]
        return {v: row.get(v) for v in variables}
