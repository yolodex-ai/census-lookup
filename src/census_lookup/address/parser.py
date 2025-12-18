"""Address parsing using usaddress library."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import usaddress


@dataclass
class ParsedAddress:
    """Parsed address components."""

    house_number: Optional[str] = None
    street_name_pre_directional: Optional[str] = None
    street_name_pre_modifier: Optional[str] = None
    street_name_pre_type: Optional[str] = None
    street_name: Optional[str] = None
    street_name_post_type: Optional[str] = None  # Ave, St, Blvd
    street_name_post_directional: Optional[str] = None
    subaddress_type: Optional[str] = None  # Apt, Suite, Unit
    subaddress_identifier: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zipcode: Optional[str] = None

    # Raw components from usaddress
    raw_components: Dict[str, str] = field(default_factory=dict)

    @property
    def full_street_name(self) -> str:
        """Combine street name components into full street name."""
        parts = [
            self.street_name_pre_directional,
            self.street_name_pre_modifier,
            self.street_name_pre_type,
            self.street_name,
            self.street_name_post_type,
            self.street_name_post_directional,
        ]
        return " ".join(p for p in parts if p)

    @property
    def has_street_info(self) -> bool:
        """Check if we have enough street information for geocoding."""
        return bool(self.street_name and self.house_number)

    def to_dict(self) -> Dict[str, Optional[str]]:
        """Convert to dictionary."""
        return {
            "house_number": self.house_number,
            "street_name_pre_directional": self.street_name_pre_directional,
            "street_name": self.street_name,
            "street_name_post_type": self.street_name_post_type,
            "street_name_post_directional": self.street_name_post_directional,
            "subaddress_type": self.subaddress_type,
            "subaddress_identifier": self.subaddress_identifier,
            "city": self.city,
            "state": self.state,
            "zipcode": self.zipcode,
        }


class AddressParseError(Exception):
    """Error parsing an address."""

    def __init__(self, address: str, reason: str):
        self.address = address
        self.reason = reason
        super().__init__(f"Failed to parse '{address}': {reason}")


class AddressParser:
    """
    Parse unstructured address strings using usaddress library.

    usaddress uses CRF (Conditional Random Fields) for probabilistic
    parsing that handles edge cases better than rule-based parsers.
    """

    # Mapping from usaddress labels to our normalized labels
    LABEL_MAP = {
        "AddressNumber": "house_number",
        "AddressNumberPrefix": "house_number_prefix",
        "AddressNumberSuffix": "house_number_suffix",
        "StreetNamePreDirectional": "street_name_pre_directional",
        "StreetNamePreModifier": "street_name_pre_modifier",
        "StreetNamePreType": "street_name_pre_type",
        "StreetName": "street_name",
        "StreetNamePostType": "street_name_post_type",
        "StreetNamePostDirectional": "street_name_post_directional",
        "SubaddressType": "subaddress_type",
        "SubaddressIdentifier": "subaddress_identifier",
        "OccupancyType": "subaddress_type",
        "OccupancyIdentifier": "subaddress_identifier",
        "PlaceName": "city",
        "StateName": "state",
        "ZipCode": "zipcode",
    }

    def parse(self, address: str) -> ParsedAddress:
        """
        Parse an address string.

        Args:
            address: Full address string

        Returns:
            ParsedAddress with extracted components

        Raises:
            AddressParseError: If parsing fails completely
        """
        if not address or not address.strip():
            raise AddressParseError(address, "Empty address")

        try:
            tagged, _ = usaddress.tag(address)
            return self._to_parsed_address(tagged)
        except usaddress.RepeatedLabelError:
            # Handle repeated labels by using parse() instead
            parsed = usaddress.parse(address)
            return self._from_parse_result(parsed)

    def _to_parsed_address(self, tagged: Dict[str, str]) -> ParsedAddress:
        """Convert usaddress tagged dict to ParsedAddress."""
        result = ParsedAddress(raw_components=dict(tagged))

        for usaddress_label, value in tagged.items():
            our_label = self.LABEL_MAP.get(usaddress_label)
            if our_label and hasattr(result, our_label):
                setattr(result, our_label, value)

        return result

    def _from_parse_result(self, parsed: List[Tuple[str, str]]) -> ParsedAddress:
        """Convert usaddress parse() result (list of tuples) to ParsedAddress."""
        # Group by label, taking first occurrence
        grouped: Dict[str, str] = {}
        for value, label in parsed:
            if label not in grouped:
                grouped[label] = value

        return self._to_parsed_address(grouped)
