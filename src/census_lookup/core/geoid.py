"""GEOID parsing and manipulation utilities."""

from dataclasses import dataclass
from enum import Enum


class GeoLevel(Enum):
    """Geographic hierarchy levels."""

    STATE = "state"
    COUNTY = "county"
    TRACT = "tract"
    BLOCK_GROUP = "block_group"
    BLOCK = "block"

    @property
    def geoid_length(self) -> int:
        """Return the GEOID length for this level."""
        lengths = {
            GeoLevel.STATE: 2,
            GeoLevel.COUNTY: 5,
            GeoLevel.TRACT: 11,
            GeoLevel.BLOCK_GROUP: 12,
            GeoLevel.BLOCK: 15,
        }
        return lengths[self]

    @classmethod
    def from_geoid_length(cls, length: int) -> "GeoLevel":
        """Determine GeoLevel from GEOID length."""
        if length >= 15:
            return cls.BLOCK
        elif length >= 12:
            return cls.BLOCK_GROUP
        elif length >= 11:
            return cls.TRACT
        elif length >= 5:
            return cls.COUNTY
        else:
            return cls.STATE


@dataclass
class GEOIDComponents:
    """Parsed GEOID components.

    Always created from a full block GEOID (15 digits), so all components
    are guaranteed to be set.
    """

    state: str  # 2 digits
    county: str  # 3 digits
    tract: str  # 6 digits
    block_group: str  # 1 digit
    block: str  # 4 digits (includes block_group)

    @property
    def county_fips(self) -> str:
        """Return full county FIPS code (state + county)."""
        return self.state + self.county

    @property
    def tract_geoid(self) -> str:
        """Return tract GEOID."""
        return self.state + self.county + self.tract

    @property
    def block_group_geoid(self) -> str:
        """Return block group GEOID."""
        return self.state + self.county + self.tract + self.block_group


class GEOIDParser:
    """
    Parse and manipulate GEOIDs.

    GEOID structure:
    - State: 2 digits (e.g., "06" for California)
    - County: 3 digits (e.g., "037" for Los Angeles)
    - Tract: 6 digits (e.g., "101100")
    - Block Group: 1 digit (e.g., "1")
    - Block: 4 digits (e.g., "1001")

    Full block GEOID: 15 digits (e.g., "060371011001001")
    """

    @staticmethod
    def parse(geoid: str) -> GEOIDComponents:
        """
        Parse a full block GEOID (15 digits) into components.

        The GEOID is validated at data load time (in converter.py), so this
        method assumes valid input.

        Args:
            geoid: A 15-digit block GEOID string

        Returns:
            GEOIDComponents with all values set
        """
        return GEOIDComponents(
            state=geoid[:2],
            county=geoid[2:5],
            tract=geoid[5:11],
            block_group=geoid[11:12],
            block=geoid[11:15],
        )
