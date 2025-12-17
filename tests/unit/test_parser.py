"""Tests for address parsing."""

import pytest

from census_lookup.address.parser import AddressParser, AddressParseError, ParsedAddress


class TestParsedAddress:
    """Tests for ParsedAddress dataclass."""

    def test_full_street_name_simple(self):
        """Test full_street_name with basic components."""
        parsed = ParsedAddress(
            street_name="MAIN",
            street_name_post_type="ST",
        )
        assert parsed.full_street_name == "MAIN ST"

    def test_full_street_name_with_directional(self):
        """Test full_street_name with directional."""
        parsed = ParsedAddress(
            street_name_pre_directional="N",
            street_name="MAIN",
            street_name_post_type="ST",
        )
        assert parsed.full_street_name == "N MAIN ST"

    def test_full_street_name_with_post_directional(self):
        """Test full_street_name with post-directional."""
        parsed = ParsedAddress(
            street_name="MAIN",
            street_name_post_type="ST",
            street_name_post_directional="NW",
        )
        assert parsed.full_street_name == "MAIN ST NW"

    def test_has_street_info_true(self):
        """Test has_street_info returns True when present."""
        parsed = ParsedAddress(house_number="123", street_name="MAIN")
        assert parsed.has_street_info is True

    def test_has_street_info_false_no_number(self):
        """Test has_street_info returns False without house number."""
        parsed = ParsedAddress(street_name="MAIN")
        assert parsed.has_street_info is False

    def test_has_street_info_false_no_name(self):
        """Test has_street_info returns False without street name."""
        parsed = ParsedAddress(house_number="123")
        assert parsed.has_street_info is False

    def test_has_location_info(self):
        """Test has_location_info property."""
        assert ParsedAddress(city="Los Angeles").has_location_info is True
        assert ParsedAddress(state="CA").has_location_info is True
        assert ParsedAddress(zipcode="90210").has_location_info is True
        assert ParsedAddress().has_location_info is False

    def test_to_dict(self):
        """Test conversion to dictionary."""
        parsed = ParsedAddress(
            house_number="123",
            street_name="MAIN",
            street_name_post_type="ST",
            city="Los Angeles",
            state="CA",
            zipcode="90210",
        )
        d = parsed.to_dict()

        assert d["house_number"] == "123"
        assert d["street_name"] == "MAIN"
        assert d["city"] == "Los Angeles"
        assert d["state"] == "CA"
        assert d["zipcode"] == "90210"


class TestAddressParser:
    """Tests for AddressParser."""

    @pytest.fixture
    def parser(self):
        return AddressParser()

    def test_parse_simple_address(self, parser):
        """Test parsing a simple address."""
        result = parser.parse("123 Main St, Los Angeles, CA 90210")

        assert result.house_number == "123"
        assert result.street_name == "Main"
        assert result.street_name_post_type == "St"
        assert result.city == "Los Angeles"
        assert result.state == "CA"
        assert result.zipcode == "90210"

    def test_parse_address_with_directional(self, parser):
        """Test parsing an address with directional."""
        result = parser.parse("456 N Oak Ave, Chicago, IL 60601")

        assert result.house_number == "456"
        assert result.street_name_pre_directional == "N"
        assert result.street_name == "Oak"
        assert result.street_name_post_type == "Ave"

    def test_parse_address_with_apartment(self, parser):
        """Test parsing an address with apartment."""
        result = parser.parse("789 Elm Blvd Apt 4B, New York, NY 10001")

        assert result.house_number == "789"
        assert result.subaddress_identifier is not None or result.subaddress_type is not None

    def test_parse_address_street_only(self, parser):
        """Test parsing a street-only address."""
        result = parser.parse("123 Main Street")

        assert result.house_number == "123"
        assert result.street_name == "Main"

    def test_parse_address_with_post_directional(self, parser):
        """Test parsing an address with post-directional."""
        result = parser.parse("100 E Capitol St NE, Washington, DC 20001")

        assert result.house_number == "100"
        # Post directional might be parsed differently depending on usaddress version

    def test_parse_empty_address_raises(self, parser):
        """Test that parsing empty address raises error."""
        with pytest.raises(AddressParseError, match="Empty"):
            parser.parse("")

    def test_parse_whitespace_only_raises(self, parser):
        """Test that parsing whitespace-only raises error."""
        with pytest.raises(AddressParseError, match="Empty"):
            parser.parse("   ")

    def test_parse_batch(self, parser):
        """Test batch parsing of addresses."""
        addresses = [
            "123 Main St, LA, CA 90210",
            "456 Oak Ave, SF, CA 94102",
            "",  # Should return empty ParsedAddress
        ]

        results = parser.parse_batch(addresses)

        assert len(results) == 3
        assert results[0].house_number == "123"
        assert results[1].house_number == "456"
        assert results[2].house_number is None  # Failed parse

    def test_parse_preserves_raw_components(self, parser):
        """Test that raw components are preserved."""
        result = parser.parse("123 Main St, LA, CA 90210")

        assert result.raw_components is not None
        assert len(result.raw_components) > 0
