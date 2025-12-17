"""GEOID parsing and manipulation utilities."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


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
    """Parsed GEOID components."""

    state: str  # 2 digits
    county: Optional[str] = None  # 3 digits
    tract: Optional[str] = None  # 6 digits
    block_group: Optional[str] = None  # 1 digit
    block: Optional[str] = None  # 4 digits (includes block_group)

    @property
    def full_geoid(self) -> str:
        """Reconstruct full GEOID from components."""
        parts = [self.state]
        if self.county:
            parts.append(self.county)
        if self.tract:
            parts.append(self.tract)
        if self.block_group and not self.block:
            parts.append(self.block_group)
        if self.block:
            parts.append(self.block)
        return "".join(parts)

    @property
    def state_fips(self) -> str:
        """Return state FIPS code."""
        return self.state

    @property
    def county_fips(self) -> Optional[str]:
        """Return full county FIPS code (state + county)."""
        if self.county:
            return self.state + self.county
        return None

    @property
    def tract_geoid(self) -> Optional[str]:
        """Return tract GEOID."""
        if self.tract:
            return self.state + (self.county or "") + self.tract
        return None

    @property
    def block_group_geoid(self) -> Optional[str]:
        """Return block group GEOID."""
        if self.block_group:
            return self.state + (self.county or "") + (self.tract or "") + self.block_group
        return None


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
        Parse a GEOID into components.

        Args:
            geoid: A GEOID string of 2-15 digits

        Returns:
            GEOIDComponents with parsed values

        Raises:
            ValueError: If GEOID is invalid
        """
        if not geoid or len(geoid) < 2:
            raise ValueError(f"Invalid GEOID: {geoid!r} (must be at least 2 digits)")

        if not geoid.isdigit():
            raise ValueError(f"Invalid GEOID: {geoid!r} (must contain only digits)")

        components = GEOIDComponents(state=geoid[:2])

        if len(geoid) >= 5:
            components.county = geoid[2:5]
        if len(geoid) >= 11:
            components.tract = geoid[5:11]
        if len(geoid) >= 12:
            components.block_group = geoid[11:12]
        if len(geoid) >= 15:
            components.block = geoid[11:15]  # Block includes block_group digit

        return components

    @staticmethod
    def truncate(geoid: str, level: GeoLevel) -> str:
        """
        Truncate GEOID to specified geographic level.

        Args:
            geoid: Full GEOID string
            level: Target geographic level

        Returns:
            Truncated GEOID string
        """
        return geoid[: level.geoid_length]

    @staticmethod
    def get_parent(geoid: str, parent_level: GeoLevel) -> str:
        """
        Get parent GEOID at specified level.

        Args:
            geoid: Child GEOID string
            parent_level: Parent geographic level

        Returns:
            Parent GEOID string
        """
        return GEOIDParser.truncate(geoid, parent_level)

    @staticmethod
    def get_level(geoid: str) -> GeoLevel:
        """
        Determine the geographic level of a GEOID based on its length.

        Args:
            geoid: GEOID string

        Returns:
            GeoLevel corresponding to the GEOID length
        """
        length = len(geoid)
        if length >= 15:
            return GeoLevel.BLOCK
        elif length >= 12:
            return GeoLevel.BLOCK_GROUP
        elif length >= 11:
            return GeoLevel.TRACT
        elif length >= 5:
            return GeoLevel.COUNTY
        else:
            return GeoLevel.STATE

    @staticmethod
    def validate(geoid: str, level: Optional[GeoLevel] = None) -> bool:
        """
        Validate a GEOID string.

        Args:
            geoid: GEOID to validate
            level: Optional expected level (checks length matches)

        Returns:
            True if valid, False otherwise
        """
        if not geoid or not geoid.isdigit():
            return False

        if len(geoid) < 2:
            return False

        if level is not None:
            return len(geoid) == level.geoid_length

        # Valid lengths: 2, 5, 11, 12, 15
        return len(geoid) in {2, 5, 11, 12, 15}
