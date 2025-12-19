"""Helper functions for functional tests with mocked HTTP responses.

This module provides helper functions for creating mock data:
- TIGER shapefile data (blocks, address features)
- Census data (PL 94-171, ACS)

All mock data uses synthetic but realistic data for Washington DC.
"""

import io
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon

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
    main_block = Polygon(
        [
            (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT - 0.005),
            (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT - 0.005),
            (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT + 0.005),
            (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT + 0.005),
        ]
    )
    blocks.append(main_block)
    geoids.append(TEST_BLOCK_GEOID)

    # Adjacent blocks for realism
    for i, (dx, dy) in enumerate([(-0.01, 0), (0.01, 0), (0, -0.01), (0, 0.01)]):
        block = Polygon(
            [
                (WHITE_HOUSE_LON + dx - 0.005, WHITE_HOUSE_LAT + dy - 0.005),
                (WHITE_HOUSE_LON + dx + 0.005, WHITE_HOUSE_LAT + dy - 0.005),
                (WHITE_HOUSE_LON + dx + 0.005, WHITE_HOUSE_LAT + dy + 0.005),
                (WHITE_HOUSE_LON + dx - 0.005, WHITE_HOUSE_LAT + dy + 0.005),
            ]
        )
        blocks.append(block)
        # Different block numbers
        geoids.append(f"11001006202100{i + 1}")

    # Add an L-shaped (concave) block to test bbox vs actual polygon intersection
    # The "notch" of the L is at the top-right corner
    # Point at (lon+0.024, lat+0.024) is in bbox but NOT in polygon
    l_block = Polygon(
        [
            (WHITE_HOUSE_LON + 0.02, WHITE_HOUSE_LAT + 0.02),  # bottom-left
            (WHITE_HOUSE_LON + 0.03, WHITE_HOUSE_LAT + 0.02),  # bottom-right
            (WHITE_HOUSE_LON + 0.03, WHITE_HOUSE_LAT + 0.025),  # mid-right
            (WHITE_HOUSE_LON + 0.025, WHITE_HOUSE_LAT + 0.025),  # notch inner
            (WHITE_HOUSE_LON + 0.025, WHITE_HOUSE_LAT + 0.03),  # notch top
            (WHITE_HOUSE_LON + 0.02, WHITE_HOUSE_LAT + 0.03),  # top-left
        ]
    )
    blocks.append(l_block)
    geoids.append("110010062021005")

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
    penn_ave = LineString(
        [
            (WHITE_HOUSE_LON - 0.01, WHITE_HOUSE_LAT),
            (WHITE_HOUSE_LON + 0.01, WHITE_HOUSE_LAT),
        ]
    )
    features.append(
        {
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
        }
    )

    # Maryland Avenue segment for batch testing
    maryland_ave = LineString(
        [
            (WHITE_HOUSE_LON - 0.02, WHITE_HOUSE_LAT - 0.01),
            (WHITE_HOUSE_LON, WHITE_HOUSE_LAT - 0.01),
        ]
    )
    features.append(
        {
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
        }
    )

    # --- Edge case features ---

    # Feature with empty FULLNAME (tests line 70 skip)
    empty_name_street = LineString(
        [
            (WHITE_HOUSE_LON + 0.02, WHITE_HOUSE_LAT),
            (WHITE_HOUSE_LON + 0.03, WHITE_HOUSE_LAT),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567892",
            "FULLNAME": "",  # Empty street name - should be skipped
            "LFROMHN": "100",
            "LTOHN": "200",
            "RFROMHN": "101",
            "RTOHN": "201",
            "ZIPL": "20500",
            "ZIPR": "20500",
            "PARITYL": "E",
            "PARITYR": "O",
            "geometry": empty_name_street,
        }
    )

    # Feature with invalid house number ranges (non-numeric)
    # Tests lines 203-204 and 218-219 (ValueError/TypeError)
    invalid_range_street = LineString(
        [
            (WHITE_HOUSE_LON - 0.03, WHITE_HOUSE_LAT + 0.01),
            (WHITE_HOUSE_LON - 0.02, WHITE_HOUSE_LAT + 0.01),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567893",
            "FULLNAME": "CONSTITUTION AVE NW",
            "LFROMHN": "INVALID",  # Non-numeric - triggers except clause
            "LTOHN": "INVALID",
            "RFROMHN": "BAD",
            "RTOHN": "BAD",
            "ZIPL": "20001",
            "ZIPR": "20001",
            "PARITYL": "E",
            "PARITYR": "O",
            "geometry": invalid_range_street,
        }
    )

    # Another Constitution Ave segment with valid ranges for fallback matching
    # but with unknown parity value (tests line 250)
    constitution_valid = LineString(
        [
            (WHITE_HOUSE_LON - 0.02, WHITE_HOUSE_LAT + 0.01),
            (WHITE_HOUSE_LON - 0.01, WHITE_HOUSE_LAT + 0.01),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567894",
            "FULLNAME": "CONSTITUTION AVE NW",
            "LFROMHN": "500",
            "LTOHN": "698",
            "RFROMHN": "501",
            "RTOHN": "699",
            "ZIPL": "20001",
            "ZIPR": "20001",
            "PARITYL": "X",  # Unknown parity - triggers else branch line 250
            "PARITYR": "X",
            "geometry": constitution_valid,
        }
    )

    # Segment with equal from/to range (tests line 273: to_addr == from_addr)
    single_addr_street = LineString(
        [
            (WHITE_HOUSE_LON + 0.01, WHITE_HOUSE_LAT + 0.02),
            (WHITE_HOUSE_LON + 0.02, WHITE_HOUSE_LAT + 0.02),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567895",
            "FULLNAME": "SINGLE ST NW",
            "LFROMHN": "100",
            "LTOHN": "100",  # Same as from - single address
            "RFROMHN": "101",
            "RTOHN": "101",
            "ZIPL": "20002",
            "ZIPR": "20002",
            "PARITYL": "E",
            "PARITYR": "O",
            "geometry": single_addr_street,
        }
    )

    # Segment with parity=B (both) to test line 239
    both_parity_street = LineString(
        [
            (WHITE_HOUSE_LON - 0.03, WHITE_HOUSE_LAT + 0.02),
            (WHITE_HOUSE_LON - 0.02, WHITE_HOUSE_LAT + 0.02),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567896",
            "FULLNAME": "BOTH ST NW",
            "LFROMHN": "1",
            "LTOHN": "99",
            "RFROMHN": "2",
            "RTOHN": "100",
            "ZIPL": "20003",
            "ZIPR": "20003",
            "PARITYL": "B",  # Both parities allowed - tests line 239
            "PARITYR": "B",
            "geometry": both_parity_street,
        }
    )

    # Segment to test right side parity failure (line 226->229):
    # - Left range: 200-298 EVEN only (PARITYL='E')
    # - Right range: 201-299 ODD only (PARITYR='O')
    # If we lookup 255 (odd):
    #   - NOT in left range (255 > 298? No, 255 < 298, but parity fails since 255 is odd)
    #   - Then check right side: 255 is in 201-299, but PARITYR='O' means odd only
    #   - Since 255 is odd, it SHOULD match right side...
    # Actually I need a case where right side parity FAILS
    # Let's use address 260: even number
    #   - Left side: 200-298 with PARITYL='E' -> 260 is even, matches!
    # That won't work either. I need left to fail AND right to fail.
    #
    # Better approach: Left range is None, only right range exists with wrong parity
    right_only_street = LineString(
        [
            (WHITE_HOUSE_LON + 0.03, WHITE_HOUSE_LAT + 0.01),
            (WHITE_HOUSE_LON + 0.04, WHITE_HOUSE_LAT + 0.01),
        ]
    )
    features.append(
        {
            "LINEARID": "1101234567897",
            "FULLNAME": "RIGHTONLY ST NW",
            "LFROMHN": None,  # No left side range
            "LTOHN": None,
            "RFROMHN": "301",
            "RTOHN": "399",
            "ZIPL": "20004",
            "ZIPR": "20004",
            "PARITYL": None,
            "PARITYR": "O",  # Odd only on right side
            "geometry": right_only_street,
        }
    )

    return gpd.GeoDataFrame(features, crs="EPSG:4269")


def create_dc_census_df() -> pd.DataFrame:
    """Create synthetic PL 94-171 census data for DC blocks."""
    # Create census data for all our test blocks (including L-shaped block)
    geoids = [TEST_BLOCK_GEOID] + [f"11001006202100{i}" for i in range(1, 6)]

    return pd.DataFrame(
        {
            "GEOID": geoids,
            "P1_001N": [150, 200, 180, 160, 190, 175],  # Total population
            "P1_003N": [80, 100, 90, 85, 95, 88],  # White alone
            "P1_004N": [40, 60, 50, 45, 55, 48],  # Black alone
            "P2_002N": [30, 40, 35, 30, 40, 35],  # Hispanic
            "H1_001N": [60, 80, 70, 65, 75, 68],  # Total housing units
            "H1_002N": [55, 75, 65, 60, 70, 63],  # Occupied
            "H1_003N": [5, 5, 5, 5, 5, 5],  # Vacant
        }
    )


def create_pl94171_zip(state_abbrev: str, census_df: pd.DataFrame) -> bytes:
    """Create a PL 94-171 format zip file for testing.

    The zip contains pipe-delimited text files:
    - Geographic header file (xxgeo2020.pl)
    - Segment 1 (xx000012020.pl) - P1, P2 tables
    - Segment 2 (xx000022020.pl) - P3, P4, H1 tables

    Args:
        state_abbrev: 2-letter state abbreviation (lowercase)
        census_df: DataFrame with GEOID and census variables

    Returns:
        ZIP file bytes
    """
    # Build the geo file content (pipe-delimited)
    # Format: Many columns, we care about positions 2 (SUMLEV), 7 (LOGRECNO), 9 (GEOID)
    geo_lines = []

    # Add a state-level record (SUMLEV=040) that won't match block filter (750)
    # This covers the branch where sumlev != summary_level (line 127->123)
    state_fields = ["" for _ in range(20)]
    state_fields[2] = "040"  # State level - won't match "750"
    state_fields[7] = "0000001"
    state_fields[9] = "0400000US11"  # State-level GEOID format
    geo_lines.append("|".join(state_fields))

    # Add a record with GEOID that doesn't have "US" prefix (alternate format)
    # This covers the branch where "US" not in geoid (line 132->135)
    alt_fields = ["" for _ in range(20)]
    alt_fields[2] = "750"  # Block level - will match
    alt_fields[7] = "0000002"
    alt_fields[9] = "110010062021099"  # Direct GEOID without "US" prefix
    geo_lines.append("|".join(alt_fields))

    for i, row in enumerate(census_df.itertuples(), start=1):
        # Build a line with enough pipe-delimited fields
        # Positions: 0, 1, 2=SUMLEV, 3, 4, 5, 6, 7=LOGRECNO, 8, 9=GEOID, ...
        geoid = row.GEOID
        sumlev = "750"  # Block level
        logrecno = str(i + 2).zfill(7)  # Zero-padded, offset by 2 for extra records

        # GEOID format in file is like "7500000US110010062021009"
        full_geoid = f"7500000US{geoid}"

        # Create a line with enough fields (need at least 10)
        fields = ["" for _ in range(20)]
        fields[2] = sumlev
        fields[7] = logrecno
        fields[9] = full_geoid
        geo_lines.append("|".join(fields))

    geo_content = "\n".join(geo_lines)

    # Build segment 1 content (P1, P2 tables)
    # Format: FILEID|STUSAB|CHAESSION|CIESSION|LOGRECNO|P1_001N|P1_002N|...|P2_001N|...
    seg1_lines = []
    for i, row in enumerate(census_df.itertuples(), start=1):
        logrecno = str(i).zfill(7)
        # First 5 columns: FILEID, STUSAB, CHAESSION, CIESSION, LOGRECNO
        fields = ["PL94171", state_abbrev.upper(), "000", "00", logrecno]

        # P1 table: 71 columns (P1_001N through P1_071N)
        for j in range(1, 72):
            col_name = f"P1_{j:03d}N"
            val = getattr(row, col_name, 0) if hasattr(row, col_name) else 0
            fields.append(str(int(val) if val else 0))

        # P2 table: 73 columns (P2_001N through P2_073N)
        for j in range(1, 74):
            col_name = f"P2_{j:03d}N"
            val = getattr(row, col_name, 0) if hasattr(row, col_name) else 0
            fields.append(str(int(val) if val else 0))

        seg1_lines.append("|".join(fields))

    seg1_content = "\n".join(seg1_lines)

    # Build segment 2 content (P3, P4, H1 tables)
    seg2_lines = []
    for i, row in enumerate(census_df.itertuples(), start=1):
        logrecno = str(i).zfill(7)
        fields = ["PL94171", state_abbrev.upper(), "000", "00", logrecno]

        # P3 table: 71 columns
        for j in range(1, 72):
            fields.append("0")

        # P4 table: 73 columns
        for j in range(1, 74):
            fields.append("0")

        # H1 table: 3 columns (H1_001N, H1_002N, H1_003N)
        for col in ["H1_001N", "H1_002N", "H1_003N"]:
            val = getattr(row, col, 0) if hasattr(row, col) else 0
            fields.append(str(int(val) if val else 0))

        seg2_lines.append("|".join(fields))

    seg2_content = "\n".join(seg2_lines)

    # Create ZIP file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{state_abbrev}geo2020.pl", geo_content.encode("latin-1"))
        zf.writestr(f"{state_abbrev}000012020.pl", seg1_content.encode("latin-1"))
        zf.writestr(f"{state_abbrev}000022020.pl", seg2_content.encode("latin-1"))

    return zip_buffer.getvalue()


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


def create_invalid_blocks_gdf() -> gpd.GeoDataFrame:
    """Create block data with invalid GEOID20 values (wrong length)."""
    blocks = []
    geoids = []

    # Block with invalid GEOID (only 10 digits instead of 15)
    main_block = Polygon(
        [
            (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT - 0.005),
            (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT - 0.005),
            (WHITE_HOUSE_LON + 0.005, WHITE_HOUSE_LAT + 0.005),
            (WHITE_HOUSE_LON - 0.005, WHITE_HOUSE_LAT + 0.005),
        ]
    )
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
