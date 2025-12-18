"""Parser for PL 94-171 legacy format files.

The Census Bureau provides PL 94-171 redistricting data in a "legacy format"
consisting of pipe-delimited text files. Each state's zip contains:
- Geographic header file (xxgeo2020.pl)
- Segment 1 (xx000012020.pl) - Tables P1 and P2
- Segment 2 (xx000022020.pl) - Tables P3, P4, and H1
- Segment 3 (xx000032020.pl) - Table P5

Reference: https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html
"""

import zipfile
from pathlib import Path
from typing import Dict, List, cast

import pandas as pd

# Summary level codes for different geographic levels
SUMMARY_LEVELS = {
    "040": "state",
    "050": "county",
    "140": "tract",
    "150": "block_group",
    "750": "block",
}

# Column positions in segment files (0-indexed after splitting by |)
# First 5 columns are: FILEID, STUSAB, CHAESSION, CIESSION, LOGRECNO
SEGMENT1_HEADER = ["FILEID", "STUSAB", "CHAESSION", "CIESSION", "LOGRECNO"]

# P1 table: 71 columns (P1_001N through P1_071N)
# Starts at column index 5
P1_COLUMNS = [f"P1_{i:03d}N" for i in range(1, 72)]

# P2 table: 73 columns (P2_001N through P2_073N)
# Starts after P1 at column index 5 + 71 = 76
P2_COLUMNS = [f"P2_{i:03d}N" for i in range(1, 74)]

SEGMENT1_COLUMNS = SEGMENT1_HEADER + P1_COLUMNS + P2_COLUMNS


# Segment 2 columns
SEGMENT2_HEADER = ["FILEID", "STUSAB", "CHARESSION", "CIESSION", "LOGRECNO"]

# P3 table: 71 columns
P3_COLUMNS = [f"P3_{i:03d}N" for i in range(1, 72)]

# P4 table: 73 columns
P4_COLUMNS = [f"P4_{i:03d}N" for i in range(1, 74)]

# H1 table: 3 columns
H1_COLUMNS = ["H1_001N", "H1_002N", "H1_003N"]

SEGMENT2_COLUMNS = SEGMENT2_HEADER + P3_COLUMNS + P4_COLUMNS + H1_COLUMNS


# Geographic header columns we care about
GEO_COLUMNS_POSITIONS = {
    "SUMLEV": 2,  # Summary level (040=state, 050=county, 140=tract, 750=block)
    "LOGRECNO": 7,  # Logical record number for joining
    "GEOID": 9,  # Full GEOID
}


def parse_pl94171_zip(
    zip_path: Path,
    variables: List[str],
    summary_level: str = "750",  # Default to block level
) -> pd.DataFrame:
    """
    Parse a PL 94-171 zip file and return census data for the specified level.

    Args:
        zip_path: Path to the state zip file (e.g., ca2020.pl.zip)
        variables: List of variables to include (None = all)
        summary_level: Geographic summary level (750=block, 140=tract, etc.)

    Returns:
        DataFrame with GEOID and census variables
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find state abbreviation from file names inside the zip
        filenames = zf.namelist()
        geo_file = next(f for f in filenames if f.endswith("geo2020.pl"))
        state_abbrev = geo_file[:2].lower()

        # Read geographic header to get LOGRECNO -> GEOID mapping
        geo_df = _parse_geo_file(zf, geo_file, summary_level)

        # Read segment 1 (P1, P2 tables)
        seg1_file = f"{state_abbrev}000012020.pl"
        seg1_df = _parse_segment_file(zf, seg1_file, SEGMENT1_COLUMNS)

        # Read segment 2 (P3, P4, H1 tables)
        seg2_file = f"{state_abbrev}000022020.pl"
        seg2_df = _parse_segment_file(zf, seg2_file, SEGMENT2_COLUMNS)

    # Join segments on LOGRECNO
    data_df = seg1_df.merge(seg2_df, on="LOGRECNO", how="inner", suffixes=("", "_drop"))
    data_df = data_df[[c for c in data_df.columns if not c.endswith("_drop")]]

    # Join with geo to get GEOID and filter by summary level
    result = geo_df.merge(data_df, on="LOGRECNO", how="inner")

    # Select only requested variables
    keep_cols = ["GEOID"] + [v for v in variables if v in result.columns]
    result = cast(pd.DataFrame, result[keep_cols])

    return result


def _parse_geo_file(
    zf: zipfile.ZipFile,
    filename: str,
    summary_level: str,
) -> pd.DataFrame:
    """Parse geographic header file and filter by summary level."""
    with zf.open(filename) as f:
        lines = f.read().decode("latin-1").splitlines()

    records = []
    for line in lines:
        parts = line.split("|")
        sumlev = parts[GEO_COLUMNS_POSITIONS["SUMLEV"]]

        if sumlev == summary_level:
            logrecno = parts[GEO_COLUMNS_POSITIONS["LOGRECNO"]]
            geoid = parts[GEO_COLUMNS_POSITIONS["GEOID"]]

            # Extract numeric GEOID (e.g., "110010001011000" from "7500000US...")
            if "US" in geoid:
                geoid = geoid.split("US")[1]

            records.append({"LOGRECNO": logrecno, "GEOID": geoid})

    return pd.DataFrame(records)


def _parse_segment_file(
    zf: zipfile.ZipFile,
    filename: str,
    columns: List[str],
) -> pd.DataFrame:
    """Parse a segment file (pipe-delimited)."""
    with zf.open(filename) as f:
        # Read as pipe-delimited, using only the columns we have names for
        df = pd.read_csv(
            f,
            sep="|",
            header=None,
            encoding="latin-1",
            dtype=str,
            usecols=range(len(columns)),
            names=columns,
        )

    # Convert numeric columns to int
    for col in df.columns:
        if col.startswith(("P1_", "P2_", "P3_", "P4_", "H1_")):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_pl94171_url(state_fips: str) -> str:
    """Get the download URL for a state's PL 94-171 zip file."""
    from census_lookup.data.constants import FIPS_STATES

    state_name = FIPS_STATES.get(state_fips, "")
    # Convert to URL format: spaces -> underscores
    state_url_name = state_name.replace(" ", "_")

    # File naming: lowercase 2-letter abbreviation + "2020.pl.zip"
    # We need to map FIPS to abbreviation
    state_abbrev = _fips_to_abbrev(state_fips)

    base_url = "https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171"
    return f"{base_url}/{state_url_name}/{state_abbrev}2020.pl.zip"


def _fips_to_abbrev(state_fips: str) -> str:
    """Convert state FIPS code to lowercase 2-letter abbreviation."""
    FIPS_TO_ABBREV: Dict[str, str] = {
        "01": "al",
        "02": "ak",
        "04": "az",
        "05": "ar",
        "06": "ca",
        "08": "co",
        "09": "ct",
        "10": "de",
        "11": "dc",
        "12": "fl",
        "13": "ga",
        "15": "hi",
        "16": "id",
        "17": "il",
        "18": "in",
        "19": "ia",
        "20": "ks",
        "21": "ky",
        "22": "la",
        "23": "me",
        "24": "md",
        "25": "ma",
        "26": "mi",
        "27": "mn",
        "28": "ms",
        "29": "mo",
        "30": "mt",
        "31": "ne",
        "32": "nv",
        "33": "nh",
        "34": "nj",
        "35": "nm",
        "36": "ny",
        "37": "nc",
        "38": "nd",
        "39": "oh",
        "40": "ok",
        "41": "or",
        "42": "pa",
        "44": "ri",
        "45": "sc",
        "46": "sd",
        "47": "tn",
        "48": "tx",
        "49": "ut",
        "50": "vt",
        "51": "va",
        "53": "wa",
        "54": "wv",
        "55": "wi",
        "56": "wy",
        "72": "pr",
    }
    return FIPS_TO_ABBREV.get(state_fips, state_fips.lower())
