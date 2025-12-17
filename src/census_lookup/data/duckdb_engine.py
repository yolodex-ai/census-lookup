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

        sql = f"""
        SELECT
            i.GEOID,
            {var_list}
        FROM input_geoids i
        LEFT JOIN (
            SELECT LEFT(GEOID, {geoid_length}) as GEOID, {', '.join(variables)}
            FROM {parquet_glob}
        ) c ON i.GEOID = c.GEOID
        """

        result = self.query(sql)
        self.conn.unregister("input_geoids")

        return result

    def aggregate_to_level(
        self,
        state_fips: str,
        variables: List[str],
        target_level: GeoLevel,
        source_level: GeoLevel = GeoLevel.BLOCK,
    ) -> pd.DataFrame:
        """
        Aggregate census data to a higher geographic level.

        Args:
            state_fips: State FIPS code
            variables: Variables to aggregate (will be summed)
            target_level: Target geographic level
            source_level: Source level of the data

        Returns:
            DataFrame with aggregated data at target level
        """
        parquet_path = self.get_census_parquet_path(state_fips)
        if not parquet_path.exists():
            raise FileNotFoundError(f"Census data not found for state {state_fips}")

        target_length = target_level.geoid_length

        # Build aggregation query
        agg_exprs = ", ".join([f"SUM({v}) as {v}" for v in variables])

        sql = f"""
        SELECT
            LEFT(GEOID, {target_length}) as GEOID,
            {agg_exprs}
        FROM read_parquet('{parquet_path}')
        GROUP BY LEFT(GEOID, {target_length})
        ORDER BY GEOID
        """

        return self.query(sql)

    def get_variables_for_geoid(
        self,
        geoid: str,
        variables: List[str],
    ) -> Dict[str, Optional[float]]:
        """
        Get census variables for a single GEOID.

        Args:
            geoid: Geographic identifier
            variables: Variables to retrieve

        Returns:
            Dictionary of variable values
        """
        result = self.join_census_data([geoid], variables)

        if result.empty:
            return {v: None for v in variables}

        row = result.iloc[0]
        return {v: row.get(v) for v in variables}

    def query_parquet(
        self,
        parquet_path: Union[str, Path],
        sql: str,
    ) -> pd.DataFrame:
        """
        Execute SQL query against a parquet file.

        Args:
            parquet_path: Path to parquet file
            sql: SQL query (use 'data' as the table name)

        Returns:
            DataFrame with query results
        """
        full_sql = f"""
        WITH data AS (
            SELECT * FROM read_parquet('{parquet_path}')
        )
        {sql}
        """
        return self.query(full_sql)

    def list_variables(self, state_fips: str) -> List[str]:
        """
        List available variables in the census data for a state.

        Args:
            state_fips: State FIPS code

        Returns:
            List of column names (excluding GEOID)
        """
        parquet_path = self.get_census_parquet_path(state_fips)
        if not parquet_path.exists():
            return []

        sql = f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        result = self.query(sql)

        # Filter out non-variable columns
        exclude = {"GEOID", "GEO_ID", "state", "county", "tract", "block", "block group"}
        return [
            row["column_name"]
            for _, row in result.iterrows()
            if row["column_name"] not in exclude
        ]

    def get_state_summary(
        self,
        state_fips: str,
        variables: List[str],
    ) -> Dict[str, float]:
        """
        Get summary statistics for a state.

        Args:
            state_fips: State FIPS code
            variables: Variables to summarize

        Returns:
            Dictionary with sum of each variable for the state
        """
        parquet_path = self.get_census_parquet_path(state_fips)
        if not parquet_path.exists():
            return {v: 0.0 for v in variables}

        agg_exprs = ", ".join([f"SUM({v}) as {v}" for v in variables])
        sql = f"SELECT {agg_exprs} FROM read_parquet('{parquet_path}')"
        result = self.query(sql)

        if result.empty:
            return {v: 0.0 for v in variables}

        return result.iloc[0].to_dict()

    def batch_lookup(
        self,
        df: pd.DataFrame,
        geoid_column: str,
        variables: List[str],
        geo_level: GeoLevel = GeoLevel.BLOCK,
    ) -> pd.DataFrame:
        """
        Efficient batch lookup that joins census data to an existing DataFrame.

        Args:
            df: Input DataFrame containing GEOIDs
            geoid_column: Name of the column containing GEOIDs
            variables: Census variables to add
            geo_level: Geographic level of the GEOIDs

        Returns:
            DataFrame with census variables added
        """
        # Register input DataFrame
        self.conn.register("input_data", df)

        # Get unique state FIPS codes
        geoids = df[geoid_column].dropna().unique()
        state_fips_set = set(g[:2] for g in geoids if len(g) >= 2)

        parquet_paths = []
        for state_fips in state_fips_set:
            path = self.get_census_parquet_path(state_fips)
            if path.exists():
                parquet_paths.append(str(path))

        if not parquet_paths:
            # No census data, return original with null columns
            for v in variables:
                df[v] = None
            return df

        var_list = ", ".join([f"c.{v}" for v in variables])
        geoid_length = geo_level.geoid_length
        parquet_glob = f"read_parquet([{', '.join(repr(p) for p in parquet_paths)}])"

        sql = f"""
        SELECT
            i.*,
            {var_list}
        FROM input_data i
        LEFT JOIN (
            SELECT LEFT(GEOID, {geoid_length}) as _geoid, {', '.join(variables)}
            FROM {parquet_glob}
        ) c ON i.{geoid_column} = c._geoid
        """

        result = self.query(sql)
        self.conn.unregister("input_data")

        return result
