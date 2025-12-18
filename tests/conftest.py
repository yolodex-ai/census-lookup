"""Pytest fixtures for census-lookup tests."""

import ast
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon


# =============================================================================
# Import enforcement: tests should only use the public API
# =============================================================================

# Allowed import patterns for census_lookup
# - "census_lookup" (the public API)
# - "census_lookup.cli" or "census_lookup.cli.commands" (CLI testing is allowed)
ALLOWED_IMPORT_PATTERNS = [
    r"^census_lookup$",  # Public API root
    r"^census_lookup\.cli(\..+)?$",  # CLI module and submodules
]


def _is_allowed_import(module_name: str) -> bool:
    """Check if a census_lookup import is allowed."""
    if not module_name.startswith("census_lookup"):
        return True  # Not a census_lookup import, always allowed
    return any(re.match(pattern, module_name) for pattern in ALLOWED_IMPORT_PATTERNS)


def _check_file_imports(filepath: Path) -> list[str]:
    """Check a test file for disallowed internal imports.

    Returns list of error messages for any violations found.
    """
    try:
        content = filepath.read_text()
        tree = ast.parse(content)
    except (SyntaxError, UnicodeDecodeError):
        return []  # Skip files that can't be parsed

    errors = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if not _is_allowed_import(alias.name):
                    errors.append(
                        f"{filepath}:{node.lineno}: "
                        f"Internal import not allowed: 'import {alias.name}'. "
                        f"Use 'from census_lookup import ...' instead."
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.module and not _is_allowed_import(node.module):
                names = ", ".join(a.name for a in node.names)
                errors.append(
                    f"{filepath}:{node.lineno}: "
                    f"Internal import not allowed: 'from {node.module} import {names}'. "
                    f"Use 'from census_lookup import ...' instead."
                )
    return errors


def pytest_collect_file(parent, file_path):
    """Check test files for internal imports during collection."""
    if file_path.suffix == ".py" and file_path.name.startswith("test_"):
        errors = _check_file_imports(file_path)
        if errors:
            # Raise an error during collection to fail fast
            error_msg = "\n".join(errors)
            pytest.fail(
                f"\n\nInternal import violations detected:\n{error_msg}\n\n"
                "Tests should only import from the public API:\n"
                "  - from census_lookup import CensusLookup, GeoLevel, ...\n"
                "  - from census_lookup.cli.commands import cli  (for CLI tests)\n"
            )


@pytest.fixture
def sample_address_features():
    """Create sample TIGER address features for testing."""
    return gpd.GeoDataFrame(
        {
            "LINEARID": ["110123456789", "110123456790", "110123456791"],
            "FULLNAME": ["MAIN ST", "OAK AVE", "1ST ST"],
            "LFROMHN": ["100", "1", "200"],
            "LTOHN": ["198", "99", "298"],
            "RFROMHN": ["101", "2", "201"],
            "RTOHN": ["199", "100", "299"],
            "ZIPL": ["90210", "90211", "90210"],
            "ZIPR": ["90210", "90211", "90210"],
            "PARITYL": ["E", "O", "E"],  # Even, Odd, Even
            "PARITYR": ["O", "E", "O"],  # Odd, Even, Odd
            "geometry": [
                LineString([(0, 0), (100, 0)]),
                LineString([(0, 0), (0, 100)]),
                LineString([(100, 0), (100, 100)]),
            ],
        },
        crs="EPSG:4269",
    )


@pytest.fixture
def sample_blocks():
    """Create sample census block polygons for testing."""
    return gpd.GeoDataFrame(
        {
            "GEOID20": [
                "060371011001001",
                "060371011001002",
                "060371011002001",
            ],
            "STATEFP20": ["06", "06", "06"],
            "COUNTYFP20": ["037", "037", "037"],
            "TRACTCE20": ["101100", "101100", "101100"],
            "BLOCKCE20": ["1001", "1002", "2001"],
            "ALAND20": [10000, 15000, 12000],
            "AWATER20": [0, 0, 0],
            "geometry": [
                Polygon([(0, 0), (50, 0), (50, 50), (0, 50)]),
                Polygon([(50, 0), (100, 0), (100, 50), (50, 50)]),
                Polygon([(0, 50), (100, 50), (100, 100), (0, 100)]),
            ],
        },
        crs="EPSG:4269",
    )


@pytest.fixture
def sample_census_data():
    """Create sample PL 94-171 census data for testing."""
    return pd.DataFrame(
        {
            "GEOID": [
                "060371011001001",
                "060371011001002",
                "060371011002001",
            ],
            "P1_001N": [150, 200, 175],  # Total population
            "P1_003N": [100, 120, 90],  # White alone
            "P1_004N": [30, 50, 60],  # Black alone
            "P2_002N": [20, 30, 25],  # Hispanic
            "H1_001N": [50, 80, 60],  # Total housing units
            "H1_002N": [45, 75, 55],  # Occupied
            "H1_003N": [5, 5, 5],  # Vacant
        }
    )


@pytest.fixture
def test_data_dir(tmp_path):
    """Create temporary data directory for tests."""
    data_dir = tmp_path / "census-lookup"
    data_dir.mkdir()
    (data_dir / "tiger" / "blocks").mkdir(parents=True)
    (data_dir / "tiger" / "addrfeat").mkdir(parents=True)
    (data_dir / "census" / "pl94171").mkdir(parents=True)
    return data_dir


@pytest.fixture
def sample_addresses():
    """Sample addresses for testing."""
    return [
        "123 Main St, Los Angeles, CA 90210",
        "456 Oak Ave, Beverly Hills, CA 90211",
        "789 1st St, Los Angeles, CA 90210",
        "Invalid Address",
        "",
    ]
