"""Census data handling for census-lookup."""

from census_lookup.census.variables import (
    DEFAULT_VARIABLES,
    VARIABLE_GROUPS,
    VARIABLES,
    get_variables_for_group,
)

__all__ = [
    "VARIABLES",
    "VARIABLE_GROUPS",
    "DEFAULT_VARIABLES",
    "get_variables_for_group",
]
