"""Fixtures for functional tests with mocked HTTP responses.

This module provides fixtures that mock all external HTTP calls:
- TIGER shapefile downloads (blocks, address features)
- Census API calls (PL 94-171, ACS)

All tests use synthetic but realistic data for Washington DC.
"""

import io
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Generator
from urllib.parse import unquote

import geopandas as gpd
import pandas as pd
import pytest
from aioresponses import CallbackResult, aioresponses
from shapely.geometry import LineString, Point, Polygon

# DC FIPS code
DC_STATE_FIPS = "11"
DC_COUNTY_FIPS = "11001"

# Test address: 1600 Pennsylvania Avenue NW coordinates (approximate)
WHITE_HOUSE_LON = -77.0365
WHITE_HOUSE_LAT = 38.8977

# GEOIDs for test data
TEST_BLOCK_GEOID = "110010062021009"  # DC block containing White House
TEST_TRACT_GEOID = "11001006202"
TEST_BLOCK_GROUP_GEOID = "110010062021"


def create_dc_blocks_gdf() -> gpd.GeoDataFrame:
    """Create synthetic DC block polygons that contain test coordinates."""
    # Create blocks that form a grid around the White House
    # Block containing White House (1600 Penn Ave)
    blocks = []
    geoids = []

    # Main block containing the White House
    # Create a polygon that definitely contains WHITE_HOUSE_LON, WHITE_HOUSE_LAT
    main_block = Polygon([
        (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT - 0.005),
        (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT - 0.005),
        (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT + 0.005),
        (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT + 0.005),
    ])
    blocks.append(main_block)
    geoids.append(TEST_BLOCK_GEOID)

    # Adjacent blocks for realism
    for i, (dx, dy) in enumerate([(-0.01, 0), (0.01, 0), (0, -0.01), (0, 0.01)]):
        block = Polygon([
            (WHITE_HOUSE_LON + dx - 0.005, WHITE_HOUSE_LAT + dy - 0.005),
            (WHITE_HOUSE_LON + dx + 0.005, WHITE_HOUSE_LAT + dy - 0.005),
            (WHITE_HOUSE_LON + dx + 0.005, WHITE_HOUSE_LAT + dy + 0.005),
            (WHITE_HOUSE_LON + dx - 0.005, WHITE_HOUSE_LAT + dy + 0.005),
        ])
        blocks.append(block)
        # Different block numbers
        geoids.append(f"11001006202100{i+1}")

    return gpd.GeoDataFrame(
        {
            "GEOID20": geoids,
            "STATEFP20": [DC_STATE_FIPS] * len(geoids),
            "COUNTYFP20": ["001"] * len(geoids),
            "TRACTCE20": ["006202"] * len(geoids),
            "BLOCKCE20": [g[-4:] for g in geoids],
            "ALAND20": [50000] * len(geoids),
            "AWATER20": [0] * len(geoids),
        },
        geometry=blocks,
        crs="EPSG:4269",
    )


def create_dc_addrfeat_gdf() -> gpd.GeoDataFrame:
    """Create synthetic DC address features for geocoding."""
    # Create street segments that will match "1600 Pennsylvania Avenue NW"
    features = []

    # Pennsylvania Avenue segment containing 1600
    penn_ave = LineString([
        (WHITE_HOUSE_LON - 0.01, WHITE_HOUSE_LAT),
        (WHITE_HOUSE_LON + 0.01, WHITE_HOUSE_LAT),
    ])
    features.append({
        "LINEARID": "1101234567890",
        "FULLNAME": "PENNSYLVANIA AVE NW",
        "LFROMHN": "1500",
        "LTOHN": "1698",
        "RFROMHN": "1501",
        "RTOHN": "1699",
        "ZIPL": "20500",
        "ZIPR": "20500",
        "PARITYL": "E",
        "PARITYR": "O",
        "geometry": penn_ave,
    })

    # Maryland Avenue segment for batch testing
    maryland_ave = LineString([
        (WHITE_HOUSE_LON - 0.02, WHITE_HOUSE_LAT - 0.01),
        (WHITE_HOUSE_LON, WHITE_HOUSE_LAT - 0.01),
    ])
    features.append({
        "LINEARID": "1101234567891",
        "FULLNAME": "MARYLAND AVE SW",
        "LFROMHN": "1",
        "LTOHN": "198",
        "RFROMHN": "2",
        "RTOHN": "199",
        "ZIPL": "20024",
        "ZIPR": "20024",
        "PARITYL": "O",
        "PARITYR": "E",
        "geometry": maryland_ave,
    })

    return gpd.GeoDataFrame(features, crs="EPSG:4269")


def create_dc_census_df() -> pd.DataFrame:
    """Create synthetic PL 94-171 census data for DC blocks."""
    # Create census data for all our test blocks
    geoids = [TEST_BLOCK_GEOID] + [f"11001006202100{i}" for i in range(1, 5)]

    return pd.DataFrame({
        "GEOID": geoids,
        "P1_001N": [150, 200, 180, 160, 190],  # Total population
        "P1_003N": [80, 100, 90, 85, 95],       # White alone
        "P1_004N": [40, 60, 50, 45, 55],        # Black alone
        "P2_002N": [30, 40, 35, 30, 40],        # Hispanic
        "H1_001N": [60, 80, 70, 65, 75],        # Total housing units
        "H1_002N": [55, 75, 65, 60, 70],        # Occupied
        "H1_003N": [5, 5, 5, 5, 5],             # Vacant
    })


def create_shapefile_zip(gdf: gpd.GeoDataFrame, name: str) -> bytes:
    """Create a ZIP file containing a shapefile from a GeoDataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write shapefile
        shp_path = Path(tmpdir) / f"{name}.shp"
        gdf.to_file(shp_path)

        # Create ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                file_path = Path(tmpdir) / f"{name}{ext}"
                if file_path.exists():
                    zf.write(file_path, f"{name}{ext}")

        return zip_buffer.getvalue()


def create_census_api_response(variables: list[str], geoids: list[str], data: pd.DataFrame) -> list:
    """Create Census API JSON response format."""
    # Census API returns: [header_row, data_row1, data_row2, ...]
    # Header includes GEO_ID, NAME, and requested variables
    header = ["GEO_ID", "NAME"] + variables + ["state", "county", "tract", "block"]

    rows = [header]
    for geoid in geoids:
        row_data = data[data["GEOID"] == geoid]
        if row_data.empty:
            continue
        row = row_data.iloc[0]

        # Build row
        geo_id = f"1000000US{geoid}"  # Census API format
        name = f"Block {geoid[-4:]}, Census Tract {geoid[5:11]}, DC"
        values = [str(int(row.get(v, 0))) for v in variables]

        # Geographic components
        state = geoid[:2]
        county = geoid[2:5]
        tract = geoid[5:11]
        block = geoid[11:15] if len(geoid) >= 15 else ""

        rows.append([geo_id, name] + values + [state, county, tract, block])

    return rows


def create_acs_api_response(variables: list[str], tracts: list[str]) -> list:
    """Create ACS API JSON response format."""
    header = ["GEO_ID", "NAME"] + variables + ["state", "county", "tract"]

    rows = [header]
    for tract in tracts:
        geo_id = f"1400000US{tract}"
        name = f"Census Tract {tract[5:]}, DC"

        # Generate reasonable ACS values
        values = []
        for v in variables:
            if "B19013" in v:  # Median household income
                values.append("85000")
            elif "B19301" in v:  # Per capita income
                values.append("55000")
            elif "B15003" in v:  # Education
                values.append("2500")
            else:
                values.append("1000")

        state = tract[:2]
        county = tract[2:5]
        tract_num = tract[5:]

        rows.append([geo_id, name] + values + [state, county, tract_num])

    return rows


@pytest.fixture
def mock_census_http() -> Generator[aioresponses, None, None]:
    """Mock all Census Bureau HTTP endpoints.

    This fixture intercepts:
    - TIGER block shapefile downloads
    - TIGER address feature downloads
    - PL 94-171 Census API calls
    - ACS Census API calls
    """
    # Pre-generate test data
    blocks_gdf = create_dc_blocks_gdf()
    addrfeat_gdf = create_dc_addrfeat_gdf()
    census_df = create_dc_census_df()

    # Create ZIP files
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    with aioresponses() as mocked:
        # Mock TIGER block downloads
        # URL pattern: https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{state}_tabblock20.zip
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        # Mock TIGER address feature downloads (per county)
        # URL pattern: https://www2.census.gov/geo/tiger/TIGER2020/ADDRFEAT/tl_2020_{county}_addrfeat.zip
        for county in [DC_COUNTY_FIPS]:  # Add more counties as needed
            addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{county}_addrfeat")
            addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{county}.*\.zip")
            mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # Mock PL 94-171 Census API
        # URL pattern: https://api.census.gov/data/2020/dec/pl?get=...
        def pl_callback(url, **kwargs):
            # Parse requested variables from URL query parameter
            # URL may be double-encoded, so we unquote the get param
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]

            # Return data for all DC blocks
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(
                status=200,
                payload=response,
            )

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        # Mock ACS API
        # URL pattern: https://api.census.gov/data/{year}/acs/acs5?get=...
        def acs_callback(url, **kwargs):
            # Parse requested variables from URL query parameter
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith("B")]
            else:
                requested_vars = ["B19013_001E"]

            # Return tract-level ACS data
            response = create_acs_api_response(
                requested_vars,
                [TEST_TRACT_GEOID],
            )
            return CallbackResult(
                status=200,
                payload=response,
            )

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=acs_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir(tmp_path: Path, mock_census_http) -> Path:
    """Create an isolated data directory for tests.

    This ensures tests don't use cached data from ~/.census-lookup
    """
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_lookup(isolated_data_dir: Path):
    """Create a CensusLookup instance configured to use mocked data."""
    from census_lookup import CensusLookup, GeoLevel

    return CensusLookup(
        geo_level=GeoLevel.TRACT,
        variables=["P1_001N"],
        data_dir=isolated_data_dir,
        auto_download=True,
    )


@pytest.fixture
def mock_census_http_404() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints returning 404 for all TIGER downloads."""
    with aioresponses() as mocked:
        # All block downloads return 404
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, status=404, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_404(tmp_path: Path, mock_census_http_404) -> Path:
    """Create isolated data directory for 404 error tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_500() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints returning 500 server error."""
    with aioresponses() as mocked:
        # All block downloads return 500
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, status=500, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_500(tmp_path: Path, mock_census_http_500) -> Path:
    """Create isolated data directory for 500 error tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_with_retries() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints that fail initially but succeed on retry."""
    import aiohttp

    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # First two requests fail, third succeeds (for blocks)
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset"))
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset"))
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        # Address features work normally
        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # Census API works
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=pl_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_retries(tmp_path: Path, mock_census_http_with_retries) -> Path:
    """Create isolated data directory for retry tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_slow_blocks() -> Generator[tuple, None, None]:
    """Mock Census endpoints with slow block downloads for concurrent testing.

    This fixture creates a coordinated mock that:
    1. First block download request triggers an event and waits
    2. Second block download request triggers completion
    3. Both share the same download via DownloadCoordinator

    Returns a tuple of (mocked, first_request_event, second_request_event)
    so tests can coordinate timing.
    """
    import asyncio

    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    # Events for coordinating concurrent requests
    first_request_started = asyncio.Event()
    second_request_started = asyncio.Event()
    request_count = {"blocks": 0}

    with aioresponses() as mocked:
        # Block downloads use coordination - first waits for second
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")

        async def blocks_callback(url, **kwargs):
            request_count["blocks"] += 1
            count = request_count["blocks"]

            if count == 1:
                # First request: signal and wait for second
                first_request_started.set()
                # Wait a short time for second request to start
                # If second request arrives, it will share this task via coordinator
                await asyncio.sleep(0.1)

            # Return the data
            return CallbackResult(body=blocks_zip)

        mocked.get(blocks_pattern, callback=blocks_callback, repeat=True)

        # Address features work normally
        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # Census API works
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=pl_callback, repeat=True)

        yield mocked, first_request_started, request_count


@pytest.fixture
def isolated_data_dir_for_slow_blocks(tmp_path: Path, mock_census_http_slow_blocks) -> tuple:
    """Create isolated data directory for slow blocks concurrent tests.

    Returns tuple of (data_dir, mocked, first_request_started, request_count)
    """
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)

    mocked, first_request_started, request_count = mock_census_http_slow_blocks
    return data_dir, first_request_started, request_count


def create_invalid_blocks_gdf() -> gpd.GeoDataFrame:
    """Create block data with invalid GEOID20 values (wrong length)."""
    blocks = []
    geoids = []

    # Block with invalid GEOID (only 10 digits instead of 15)
    main_block = Polygon([
        (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT - 0.005),
        (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT - 0.005),
        (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT + 0.005),
        (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT + 0.005),
    ])
    blocks.append(main_block)
    geoids.append("1100100620")  # Invalid: only 10 digits

    return gpd.GeoDataFrame(
        {
            "GEOID20": geoids,
            "STATEFP20": [DC_STATE_FIPS],
            "COUNTYFP20": ["001"],
            "TRACTCE20": ["006202"],
            "BLOCKCE20": ["0620"],
            "ALAND20": [50000],
            "AWATER20": [0],
        },
        geometry=blocks,
        crs="EPSG:4269",
    )


@pytest.fixture
def mock_census_http_invalid_geoid() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints with invalid GEOID data in blocks."""
    blocks_gdf = create_invalid_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    # Still need valid address features for the download to proceed
    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")

    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # Mock blocks with invalid GEOIDs
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        # Mock address features (valid)
        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # Mock Census API (needed for load_state)
        def pl_callback(url, **kwargs):
            from urllib.parse import unquote
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_invalid_geoid(tmp_path: Path, mock_census_http_invalid_geoid) -> Path:
    """Create isolated data directory for invalid GEOID tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_connection_errors() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints that always fail with connection errors."""
    import aiohttp

    with aioresponses() as mocked:
        # All block downloads fail with connection error (no recovery)
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        # Register multiple failures to exhaust retries (default is 3)
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset by peer"))
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset by peer"))
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset by peer"))
        mocked.get(blocks_pattern, exception=aiohttp.ClientError("Connection reset by peer"))

        yield mocked


@pytest.fixture
def isolated_data_dir_for_connection_errors(tmp_path: Path, mock_census_http_connection_errors) -> Path:
    """Create isolated data directory for connection error tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_acs_400() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints with ACS returning 400 for invalid variables."""
    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # TIGER downloads work normally
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # PL 94-171 works normally
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        # ACS returns 400 error for invalid variables
        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(
            acs_pattern,
            status=400,
            body="error: unknown variable 'INVALID_VAR'",
            repeat=True,
        )

        yield mocked


@pytest.fixture
def isolated_data_dir_for_acs_400(tmp_path: Path, mock_census_http_acs_400) -> Path:
    """Create isolated data directory for ACS 400 error tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_many_variables() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints that handle many variables (>50) with batch merging."""
    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # TIGER downloads work normally
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # PL 94-171 handles many variables - returns whatever is requested
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                # Parse all requested variables
                parts = get_param.split(",")
                requested_vars = [v for v in parts if v.startswith(("P", "H")) and v != "GEO_ID"]
            else:
                requested_vars = ["P1_001N"]

            # Build response with all requested variables
            geoids = census_df["GEOID"].tolist()
            header = ["GEO_ID", "NAME"] + requested_vars + ["state", "county", "tract", "block"]
            rows = [header]

            for geoid in geoids:
                geo_id = f"1000000US{geoid}"
                name = f"Block {geoid[-4:]}, Census Tract {geoid[5:11]}, DC"
                # Return dummy values for all variables
                values = ["100"] * len(requested_vars)
                state = geoid[:2]
                county = geoid[2:5]
                tract = geoid[5:11]
                block = geoid[11:15] if len(geoid) >= 15 else ""
                rows.append([geo_id, name] + values + [state, county, tract, block])

            return CallbackResult(status=200, payload=rows)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        # ACS not needed for this test
        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=pl_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_many_variables(tmp_path: Path, mock_census_http_many_variables) -> Path:
    """Create isolated data directory for many variables tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_many_acs_variables() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints that handle many ACS variables (>50) with batch merging."""
    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # TIGER downloads work normally
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # PL 94-171 works normally
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        # ACS handles many variables
        def acs_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                parts = get_param.split(",")
                requested_vars = [v for v in parts if v.startswith("B")]
            else:
                requested_vars = ["B19013_001E"]

            # Return tract-level ACS data with all requested variables
            tracts = [TEST_TRACT_GEOID]
            header = ["GEO_ID", "NAME"] + requested_vars + ["state", "county", "tract"]
            rows = [header]

            for tract in tracts:
                geo_id = f"1400000US{tract}"
                name = f"Census Tract {tract[5:]}, DC"
                values = ["50000"] * len(requested_vars)
                state = tract[:2]
                county = tract[2:5]
                tract_num = tract[5:]
                rows.append([geo_id, name] + values + [state, county, tract_num])

            return CallbackResult(status=200, payload=rows)

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=acs_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_many_acs_variables(tmp_path: Path, mock_census_http_many_acs_variables) -> Path:
    """Create isolated data directory for many ACS variables tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir


@pytest.fixture
def mock_census_http_with_request_counting() -> Generator[tuple, None, None]:
    """Mock Census endpoints with request counting for cache tests."""
    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    request_count = {"blocks": 0, "addrfeat": 0, "census": 0}

    with aioresponses() as mocked:
        # Block downloads with counting
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")

        def blocks_callback(url, **kwargs):
            request_count["blocks"] += 1
            return CallbackResult(body=blocks_zip)

        mocked.get(blocks_pattern, callback=blocks_callback, repeat=True)

        # Address features with counting
        addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*\.zip")

        def addrfeat_callback(url, **kwargs):
            request_count["addrfeat"] += 1
            return CallbackResult(body=addrfeat_zip)

        mocked.get(addrfeat_pattern, callback=addrfeat_callback, repeat=True)

        # Census API with counting
        def pl_callback(url, **kwargs):
            request_count["census"] += 1
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=pl_callback, repeat=True)

        yield mocked, request_count


@pytest.fixture
def isolated_data_dir_with_preextracted(tmp_path: Path, mock_census_http_with_request_counting) -> tuple:
    """Create data directory with pre-extracted block files to test cache hit."""
    mocked, request_count = mock_census_http_with_request_counting

    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)

    # Pre-extract blocks to the expected location
    blocks_gdf = create_dc_blocks_gdf()
    extract_dir = data_dir / "temp" / f"tl_2020_{DC_STATE_FIPS}_tabblock20"
    extract_dir.mkdir(parents=True)
    blocks_gdf.to_file(extract_dir / f"tl_2020_{DC_STATE_FIPS}_tabblock20.shp")

    return data_dir, request_count


@pytest.fixture
def mock_census_http_missing_county() -> Generator[aioresponses, None, None]:
    """Mock Census endpoints where some counties return 404 for address features."""
    blocks_gdf = create_dc_blocks_gdf()
    blocks_zip = create_shapefile_zip(blocks_gdf, f"tl_2020_{DC_STATE_FIPS}_tabblock20")

    addrfeat_gdf = create_dc_addrfeat_gdf()
    addrfeat_zip = create_shapefile_zip(addrfeat_gdf, f"tl_2020_{DC_COUNTY_FIPS}_addrfeat")
    census_df = create_dc_census_df()

    with aioresponses() as mocked:
        # Blocks work normally
        blocks_pattern = re.compile(r".*census\.gov.*TABBLOCK20.*\.zip")
        mocked.get(blocks_pattern, body=blocks_zip, repeat=True)

        # DC county (11001) works, but fake county 11999 returns 404
        dc_addrfeat_pattern = re.compile(rf".*census\.gov.*ADDRFEAT.*{DC_COUNTY_FIPS}.*\.zip")
        mocked.get(dc_addrfeat_pattern, body=addrfeat_zip, repeat=True)

        # Any other county returns 404
        other_addrfeat_pattern = re.compile(r".*census\.gov.*ADDRFEAT.*\.zip")
        mocked.get(other_addrfeat_pattern, status=404, repeat=True)

        # Census API works
        def pl_callback(url, **kwargs):
            get_param = unquote(url.query.get("get", ""))
            if get_param:
                requested_vars = [v for v in get_param.split(",") if v.startswith(("P", "H"))]
            else:
                requested_vars = ["P1_001N"]
            response = create_census_api_response(
                requested_vars,
                census_df["GEOID"].tolist(),
                census_df,
            )
            return CallbackResult(status=200, payload=response)

        pl_pattern = re.compile(r".*api\.census\.gov/data/2020/dec/pl.*")
        mocked.get(pl_pattern, callback=pl_callback, repeat=True)

        acs_pattern = re.compile(r".*api\.census\.gov/data/\d+/acs/acs5.*")
        mocked.get(acs_pattern, callback=pl_callback, repeat=True)

        yield mocked


@pytest.fixture
def isolated_data_dir_for_missing_county(tmp_path: Path, mock_census_http_missing_county) -> Path:
    """Create isolated data directory for missing county tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    (data_dir / "census" / "acs").mkdir(parents=True)
    (data_dir / "temp").mkdir(parents=True)
    return data_dir
