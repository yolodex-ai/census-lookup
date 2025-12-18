"""Match addresses to TIGER address range segments."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, cast

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

from census_lookup.address.normalizer import StreetNormalizer
from census_lookup.address.parser import AddressParser, ParsedAddress


@dataclass
class GeocodingResult:
    """Result from address geocoding."""

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    matched_address: Optional[str] = None
    match_type: str = "no_match"  # "exact", "interpolated", "no_match"
    match_score: float = 0.0  # 0.0 to 1.0
    tiger_line_id: Optional[str] = None
    side: Optional[str] = None  # "L" or "R"

    @property
    def is_matched(self) -> bool:
        """Check if geocoding was successful."""
        return self.match_type != "no_match" and self.latitude is not None


class TIGERAddressMatcher:
    """
    Match addresses to TIGER address range segments.

    Uses address interpolation along street segments based on
    address ranges encoded in TIGER/Line ADDRFEAT files.

    TIGER ADDRFEAT files contain:
    - FULLNAME: Full street name
    - LFROMHN, LTOHN: Left side house number range
    - RFROMHN, RTOHN: Right side house number range
    - ZIPL, ZIPR: ZIP codes for each side
    - PARITYL, PARITYR: Address parity (O=odd, E=even, B=both)
    - geometry: LineString of street segment
    """

    def __init__(self, addr_features: gpd.GeoDataFrame):
        """
        Initialize with TIGER address features.

        Args:
            addr_features: GeoDataFrame from ADDRFEAT files
        """
        self._features = addr_features.copy()
        self._normalizer = StreetNormalizer()
        self._parser = AddressParser()
        self._build_indexes()

    def _build_indexes(self) -> None:
        """Build indexes for fast street name lookup."""
        # Create normalized street name column
        self._features["_norm_name"] = self._features["FULLNAME"].apply(
            lambda x: self._normalizer.normalize(str(x) if x else "")
        )

        # Build dict: normalized_name -> list of row indices
        self._street_index: Dict[str, List[int]] = {}
        for idx, name in enumerate(self._features["_norm_name"]):
            if not name:
                continue
            if name not in self._street_index:
                self._street_index[name] = []
            self._street_index[name].append(idx)

    def geocode_parsed(self, parsed: ParsedAddress) -> GeocodingResult:
        """
        Geocode a parsed address.

        Args:
            parsed: Parsed address components

        Returns:
            GeocodingResult with coordinates and match info
        """
        if not parsed.has_street_info:
            return GeocodingResult(match_type="no_match", match_score=0.0)

        # has_street_info guarantees house_number is not None
        assert parsed.house_number is not None
        try:
            house_number = int(parsed.house_number)
        except ValueError:
            return GeocodingResult(match_type="no_match", match_score=0.0)

        # Build normalized street name for matching
        # TIGER uses abbreviated format, so normalize without expansion
        street_name = self._normalizer.normalize(parsed.full_street_name)

        # Find matching segment
        result = self._find_segment(
            house_number=house_number,
            street_name=street_name,
            zipcode=parsed.zipcode,
        )

        if result is None:
            # Try variants
            for variant in self._normalizer.generate_variants(street_name):
                result = self._find_segment(
                    house_number=house_number,
                    street_name=variant,
                    zipcode=parsed.zipcode,
                )
                if result:
                    break

        if result is None:
            return GeocodingResult(match_type="no_match", match_score=0.0)

        segment, side, from_addr, to_addr, tiger_id = result

        # Interpolate position
        geom = cast(LineString, segment.geometry)
        point = self._interpolate_position(
            segment_geom=geom,
            house_number=house_number,
            from_addr=from_addr,
            to_addr=to_addr,
        )

        return GeocodingResult(
            latitude=point.y,
            longitude=point.x,
            matched_address=segment.get("FULLNAME"),
            match_type="interpolated",
            match_score=0.9,  # Could be refined based on match quality
            tiger_line_id=tiger_id,
            side=side,
        )

    def _find_segment(
        self,
        house_number: int,
        street_name: str,
        zipcode: Optional[str] = None,
    ) -> Optional[Tuple[pd.Series, str, int, int, str]]:
        """
        Find the street segment containing an address.

        Args:
            house_number: House number to find
            street_name: Normalized street name
            zipcode: Optional ZIP code for filtering

        Returns:
            Tuple of (segment, side, from_addr, to_addr, tiger_id) or None
        """
        # Get candidate segments by street name
        candidates_idx = self._street_index.get(street_name, [])

        if not candidates_idx:
            return None

        candidates = self._features.iloc[candidates_idx]

        # Filter by ZIP if provided
        if zipcode:
            zipcode = str(zipcode).strip()
            zip_mask = (candidates["ZIPL"].astype(str) == zipcode) | (
                candidates["ZIPR"].astype(str) == zipcode
            )
            if zip_mask.any():
                candidates = candidates[zip_mask]

        # Find segment with matching address range
        for idx, segment in candidates.iterrows():
            match = self._check_range(segment, house_number)
            if match:
                side, from_addr, to_addr = match
                tiger_id = segment.get("LINEARID", str(idx))
                return segment, side, from_addr, to_addr, tiger_id

        return None

    def _check_range(
        self,
        segment: pd.Series,
        house_number: int,
    ) -> Optional[Tuple[str, int, int]]:
        """
        Check if house number falls in segment's address range.

        Considers parity (odd/even) to determine correct side.

        Args:
            segment: Row from address features GeoDataFrame
            house_number: House number to check

        Returns:
            Tuple of (side, from_addr, to_addr) or None if not in range
        """
        # Check left side
        try:
            lfrom_val = segment["LFROMHN"]
            lto_val = segment["LTOHN"]
            lfrom = int(lfrom_val) if bool(pd.notna(lfrom_val)) else None
            lto = int(lto_val) if bool(pd.notna(lto_val)) else None
        except (ValueError, TypeError):
            lfrom, lto = None, None

        if lfrom is not None and lto is not None:
            range_min, range_max = min(lfrom, lto), max(lfrom, lto)
            if range_min <= house_number <= range_max:
                # Check parity
                parity = segment.get("PARITYL", "B")
                if self._parity_matches(house_number, parity, lfrom):
                    return "L", lfrom, lto

        # Check right side
        try:
            rfrom_val = segment["RFROMHN"]
            rto_val = segment["RTOHN"]
            rfrom = int(rfrom_val) if bool(pd.notna(rfrom_val)) else None
            rto = int(rto_val) if bool(pd.notna(rto_val)) else None
        except (ValueError, TypeError):
            rfrom, rto = None, None

        if rfrom is not None and rto is not None:
            range_min, range_max = min(rfrom, rto), max(rfrom, rto)
            if range_min <= house_number <= range_max:
                # Check parity
                parity = segment.get("PARITYR", "B")
                if self._parity_matches(house_number, parity, rfrom):
                    return "R", rfrom, rto

        return None

    def _parity_matches(
        self,
        house_number: int,
        parity: Optional[str],
        range_start: int,
    ) -> bool:
        """Check if house number matches the parity requirement."""
        if parity is None or parity == "B":
            return True

        house_is_odd = house_number % 2 == 1
        range_is_odd = range_start % 2 == 1

        if parity == "O":
            return house_is_odd
        elif parity == "E":
            return not house_is_odd
        else:
            # Fall back to matching range start parity
            return house_is_odd == range_is_odd

    def _interpolate_position(
        self,
        segment_geom: LineString,
        house_number: int,
        from_addr: int,
        to_addr: int,
    ) -> Point:
        """
        Interpolate address position along segment.

        Args:
            segment_geom: Street segment LineString
            house_number: Target house number
            from_addr: Start of address range
            to_addr: End of address range

        Returns:
            Point at interpolated position
        """
        # Calculate position along segment (0.0 to 1.0)
        if to_addr == from_addr:
            position = 0.5
        else:
            position = (house_number - from_addr) / (to_addr - from_addr)

        # Clamp to valid range
        position = max(0.0, min(1.0, position))

        # Interpolate point along line
        point = segment_geom.interpolate(position, normalized=True)

        # Note: Offsetting to the correct side of the street would require
        # projecting to a meter-based CRS, which adds complexity.
        # For most geocoding purposes, the centerline position is sufficient.

        return point
