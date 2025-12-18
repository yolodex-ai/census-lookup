"""CLI commands for census-lookup."""

import asyncio
import json
from pathlib import Path
from typing import Optional

import click
import pandas as pd

from census_lookup import CensusLookup, GeoLevel
from census_lookup.census.variables import VARIABLES, list_variable_groups
from census_lookup.data.constants import FIPS_STATES, normalize_state
from census_lookup.data.manager import DataManager


@click.group()
@click.version_option(package_name="census-lookup")
def cli():
    """Census-lookup: Map US addresses to Census 2020 data."""
    pass


@cli.command()
@click.argument("address")
@click.option(
    "--level",
    "-l",
    default="block",
    type=click.Choice(["block", "block_group", "tract", "county", "state"]),
    help="Geographic level for results",
)
@click.option(
    "--variables",
    "-v",
    multiple=True,
    help="Census variables to include (can be specified multiple times)",
)
def lookup(address: str, level: str, variables: tuple[str, ...]):
    """Look up census data for a single address."""
    asyncio.run(_lookup_async(address, level, variables))


async def _lookup_async(address: str, level: str, variables: tuple[str, ...]):
    """Async implementation of lookup command."""
    geo_level = GeoLevel[level.upper()]

    lookup_instance = CensusLookup(
        geo_level=geo_level,
        variables=list(variables) if variables else None,
    )

    try:
        result = await lookup_instance.geocode(address)
        click.echo(json.dumps(result.to_dict(), indent=2, default=str))
    finally:
        await lookup_instance.close()


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option(
    "--address-column",
    "-a",
    required=True,
    help="Column containing addresses",
)
@click.option(
    "--level",
    "-l",
    default="block",
    type=click.Choice(["block", "block_group", "tract", "county"]),
)
@click.option("--variables", "-v", multiple=True, help="Census variables to include")
def batch(
    input_file: str,
    output_file: str,
    address_column: str,
    level: str,
    variables: tuple[str, ...],
):
    """Process a batch of addresses from CSV file."""
    asyncio.run(_batch_async(input_file, output_file, address_column, level, variables))


async def _batch_async(
    input_file: str,
    output_file: str,
    address_column: str,
    level: str,
    variables: tuple[str, ...],
):
    """Async implementation of batch command."""
    input_path = Path(input_file)

    # Read input
    if input_path.suffix != ".csv":
        raise click.ClickException(
            f"Unsupported file format: {input_path.suffix}. Only CSV is supported."
        )
    df = pd.read_csv(input_path)

    if address_column not in df.columns:
        raise click.ClickException(f"Column '{address_column}' not found in input file")

    # Process
    geo_level = GeoLevel[level.upper()]
    lookup_instance = CensusLookup(
        geo_level=geo_level,
        variables=list(variables) if variables else None,
    )

    try:
        # df[column] returns pd.Series for single column
        address_series: pd.Series = df[address_column]  # type: ignore[assignment]
        results = await lookup_instance.geocode_batch(
            address_series,
            progress=True,
        )

        # Merge results with original data
        output_df = pd.concat([df.reset_index(drop=True), results.reset_index(drop=True)], axis=1)

        # Save
        output_path = Path(output_file)
        if output_path.suffix == ".csv":
            output_df.to_csv(output_path, index=False)
        elif output_path.suffix == ".parquet":
            output_df.to_parquet(output_path)
        else:
            output_df.to_csv(output_path, index=False)

        click.echo(f"Processed {len(df)} addresses -> {output_file}")

        # Summary
        matched = results["match_type"].isin(["interpolated", "exact"]).sum()
        click.echo(f"Matched: {matched}/{len(df)} ({100 * matched / len(df):.1f}%)")
    finally:
        await lookup_instance.close()


@cli.command()
@click.argument("states", nargs=-1, required=True)
def download(states: tuple[str, ...]):
    """Pre-download data for specified states."""
    asyncio.run(_download_async(states))


async def _download_async(states: tuple[str, ...]):
    """Async implementation of download command."""
    manager = DataManager()

    try:
        for state in states:
            try:
                state_fips = normalize_state(state)
                state_name = FIPS_STATES.get(state_fips, state)
                click.echo(f"\nDownloading data for {state_name} ({state_fips})...")
                await manager.ensure_state_data(state_fips, show_progress=True)
                click.echo("  Done.")
            except Exception as e:
                click.echo(f"  Error: {e}", err=True)

        click.echo("\nDownload complete!")
    finally:
        await manager.close()


@cli.command()
def info():
    """Show information about downloaded data."""
    manager = DataManager()

    click.echo(f"\nData directory: {manager.data_dir}")

    # Disk usage
    usage = manager.disk_usage()
    click.echo("\nDisk usage:")
    for category, size in usage.items():
        if category != "total":
            click.echo(f"  {category}: {_format_size(size)}")
    click.echo(f"  Total: {_format_size(usage['total'])}")

    # Available states
    states = manager.list_available_states("blocks")
    if states:
        click.echo(f"\nStates with block data ({len(states)}):")
        state_names = [FIPS_STATES.get(s, s) for s in states]
        click.echo(f"  {', '.join(state_names)}")
    else:
        click.echo("\nNo data downloaded yet.")
        click.echo("Run: census-lookup download <state>")


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    size: float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


@cli.command()
@click.option("--table", "-t", help="Filter by table (P1, P2, P3, P4, H1)")
def variables(table: Optional[str]):
    """List available census variables."""
    click.echo("\nAvailable Census Variables (PL 94-171):")
    click.echo("=" * 60)

    for code, desc in sorted(VARIABLES.items()):
        if table and not code.startswith(table):
            continue
        click.echo(f"{code:12s} {desc}")

    click.echo("\n\nVariable Groups:")
    click.echo("-" * 40)
    for group, desc in list_variable_groups().items():
        click.echo(f"{group:20s} {desc}")


@cli.command()
@click.argument("state", required=False)
def clear(state: Optional[str]):
    """Clear cached data for a state or all states."""
    manager = DataManager()

    if state:
        state_fips = normalize_state(state)
        click.confirm(
            f"Clear all cached data for {FIPS_STATES.get(state_fips, state)}?",
            abort=True,
        )
        manager.clear_cache(state_fips)
        click.echo("Cache cleared.")
    else:
        click.confirm("Clear ALL cached data?", abort=True)
        manager.clear_cache()
        click.echo("All cache cleared.")


@cli.command()
@click.argument("lat", type=float)
@click.argument("lon", type=float)
@click.option(
    "--level",
    "-l",
    default="block",
    type=click.Choice(["block", "block_group", "tract", "county"]),
)
@click.option("--variables", "-v", multiple=True, help="Census variables to include")
def coords(lat: float, lon: float, level: str, variables: tuple[str, ...]):
    """Look up census data for coordinates (lat, lon)."""
    asyncio.run(_coords_async(lat, lon, level, variables))


async def _coords_async(lat: float, lon: float, level: str, variables: tuple[str, ...]):
    """Async implementation of coords command."""
    geo_level = GeoLevel[level.upper()]

    lookup_instance = CensusLookup(
        geo_level=geo_level,
        variables=list(variables) if variables else None,
    )

    try:
        # Load any previously downloaded states from the catalog
        available_states = lookup_instance._data_manager.list_available_states("blocks")
        if available_states:
            click.echo(f"Loading {len(available_states)} downloaded state(s)...")
            for state in available_states:
                await lookup_instance.load_state(state)
        else:
            click.echo("No states downloaded. Run 'census-lookup download <state>' first.")

        click.echo("Attempting to find containing state...")

        result = await lookup_instance.lookup_coordinates(lat, lon)

        if result.is_matched:
            click.echo(json.dumps(result.to_dict(), indent=2, default=str))
        else:
            click.echo("No census block found for these coordinates.")
            click.echo("Try loading the appropriate state first: census-lookup download <state>")
    finally:
        await lookup_instance.close()


if __name__ == "__main__":
    cli()
