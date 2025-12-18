"""Census 2020 PL 94-171 variable definitions."""

from typing import Dict, List

# PL 94-171 Variable Definitions
# Reference: https://www.census.gov/programs-surveys/decennial-census/about/rdo/summary-files.html

VARIABLES: Dict[str, str] = {
    # Table P1: Race
    "P1_001N": "Total Population",
    "P1_002N": "Population of one race",
    "P1_003N": "White alone",
    "P1_004N": "Black or African American alone",
    "P1_005N": "American Indian and Alaska Native alone",
    "P1_006N": "Asian alone",
    "P1_007N": "Native Hawaiian and Other Pacific Islander alone",
    "P1_008N": "Some Other Race alone",
    "P1_009N": "Population of two or more races",
    "P1_010N": "Population of two races",
    "P1_011N": "White; Black or African American",
    "P1_012N": "White; American Indian and Alaska Native",
    "P1_013N": "White; Asian",
    "P1_014N": "White; Native Hawaiian and Other Pacific Islander",
    "P1_015N": "White; Some Other Race",
    "P1_016N": "Black or African American; American Indian and Alaska Native",
    "P1_017N": "Black or African American; Asian",
    "P1_018N": "Black or African American; Native Hawaiian and Other Pacific Islander",
    "P1_019N": "Black or African American; Some Other Race",
    "P1_020N": "American Indian and Alaska Native; Asian",
    "P1_021N": "American Indian and Alaska Native; Native Hawaiian and Other Pacific Islander",
    "P1_022N": "American Indian and Alaska Native; Some Other Race",
    "P1_023N": "Asian; Native Hawaiian and Other Pacific Islander",
    "P1_024N": "Asian; Some Other Race",
    "P1_025N": "Native Hawaiian and Other Pacific Islander; Some Other Race",
    # Table P2: Hispanic or Latino by Race
    "P2_001N": "Total Population",
    "P2_002N": "Hispanic or Latino",
    "P2_003N": "Not Hispanic or Latino",
    "P2_004N": "Not Hispanic or Latino: Population of one race",
    "P2_005N": "Not Hispanic or Latino: White alone",
    "P2_006N": "Not Hispanic or Latino: Black or African American alone",
    "P2_007N": "Not Hispanic or Latino: American Indian and Alaska Native alone",
    "P2_008N": "Not Hispanic or Latino: Asian alone",
    "P2_009N": "Not Hispanic or Latino: Native Hawaiian and Other Pacific Islander alone",
    "P2_010N": "Not Hispanic or Latino: Some Other Race alone",
    "P2_011N": "Not Hispanic or Latino: Population of two or more races",
    # Table P3: Race for Population 18 Years and Over
    "P3_001N": "Total Population 18 years and over",
    "P3_002N": "Population 18+ of one race",
    "P3_003N": "Population 18+ White alone",
    "P3_004N": "Population 18+ Black or African American alone",
    "P3_005N": "Population 18+ American Indian and Alaska Native alone",
    "P3_006N": "Population 18+ Asian alone",
    "P3_007N": "Population 18+ Native Hawaiian and Other Pacific Islander alone",
    "P3_008N": "Population 18+ Some Other Race alone",
    "P3_009N": "Population 18+ of two or more races",
    # Table P4: Hispanic or Latino by Race for Population 18+
    "P4_001N": "Total Population 18 years and over",
    "P4_002N": "Hispanic or Latino 18+",
    "P4_003N": "Not Hispanic or Latino 18+",
    # Table H1: Housing Units
    "H1_001N": "Total Housing Units",
    "H1_002N": "Occupied Housing Units",
    "H1_003N": "Vacant Housing Units",
}

# Common variable groups for convenience
VARIABLE_GROUPS: Dict[str, List[str]] = {
    "population": ["P1_001N"],
    "race_simple": [
        "P1_001N",
        "P1_003N",
        "P1_004N",
        "P1_005N",
        "P1_006N",
        "P1_007N",
        "P1_008N",
    ],
    "race_detailed": [f"P1_{i:03d}N" for i in range(1, 26)],
    "hispanic": ["P2_001N", "P2_002N", "P2_003N"],
    "hispanic_detailed": [f"P2_{i:03d}N" for i in range(1, 12)],
    "voting_age": ["P3_001N"],
    "voting_age_race": [f"P3_{i:03d}N" for i in range(1, 10)],
    "housing": ["H1_001N", "H1_002N", "H1_003N"],
    "all": list(VARIABLES.keys()),
}

# Default variables to download (balance between completeness and size)
DEFAULT_VARIABLES: List[str] = [
    "P1_001N",  # Total population
    "P1_003N",  # White alone
    "P1_004N",  # Black alone
    "P1_005N",  # American Indian alone
    "P1_006N",  # Asian alone
    "P1_007N",  # Pacific Islander alone
    "P1_008N",  # Other race alone
    "P2_002N",  # Hispanic or Latino
    "P2_005N",  # Non-Hispanic White alone
    "P3_001N",  # Voting age population
    "H1_001N",  # Total housing units
    "H1_002N",  # Occupied housing units
    "H1_003N",  # Vacant housing units
]


def get_variables_for_group(group: str) -> List[str]:
    """
    Get list of variables for a named group.

    Args:
        group: Group name (e.g., "population", "race_simple", "housing")

    Returns:
        List of variable codes

    Raises:
        ValueError: If group name is not recognized
    """
    if group not in VARIABLE_GROUPS:
        valid = ", ".join(VARIABLE_GROUPS.keys())
        raise ValueError(f"Unknown variable group: {group}. Valid groups: {valid}")

    return VARIABLE_GROUPS[group]


def list_tables() -> Dict[str, str]:
    """List available PL 94-171 tables."""
    return {
        "P1": "Race",
        "P2": "Hispanic or Latino by Race",
        "P3": "Race for Population 18 Years and Over",
        "P4": "Hispanic or Latino by Race for Population 18+",
        "H1": "Housing Units",
    }


def list_variable_groups() -> Dict[str, str]:
    """List available variable groups with descriptions."""
    return {
        "population": "Total population only",
        "race_simple": "Population by major race categories",
        "race_detailed": "Population by all race combinations",
        "hispanic": "Hispanic/Latino population",
        "hispanic_detailed": "Hispanic/Latino by race",
        "voting_age": "Population 18 years and over",
        "voting_age_race": "Voting age population by race",
        "housing": "Housing unit counts",
        "all": "All available variables",
    }
