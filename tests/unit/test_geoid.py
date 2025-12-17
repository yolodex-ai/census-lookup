"""Tests for GEOID parsing and manipulation."""

import pytest

from census_lookup.core.geoid import GeoLevel, GEOIDComponents, GEOIDParser


class TestGeoLevel:
    """Tests for GeoLevel enum."""

    def test_geoid_lengths(self):
        """Test that GEOID lengths are correct."""
        assert GeoLevel.STATE.geoid_length == 2
        assert GeoLevel.COUNTY.geoid_length == 5
        assert GeoLevel.TRACT.geoid_length == 11
        assert GeoLevel.BLOCK_GROUP.geoid_length == 12
        assert GeoLevel.BLOCK.geoid_length == 15

    def test_enum_values(self):
        """Test enum string values."""
        assert GeoLevel.STATE.value == "state"
        assert GeoLevel.COUNTY.value == "county"
        assert GeoLevel.TRACT.value == "tract"
        assert GeoLevel.BLOCK_GROUP.value == "block_group"
        assert GeoLevel.BLOCK.value == "block"

    def test_from_geoid_length_exact_matches(self):
        """Test from_geoid_length with exact GEOID lengths."""
        assert GeoLevel.from_geoid_length(2) == GeoLevel.STATE
        assert GeoLevel.from_geoid_length(5) == GeoLevel.COUNTY
        assert GeoLevel.from_geoid_length(11) == GeoLevel.TRACT
        assert GeoLevel.from_geoid_length(12) == GeoLevel.BLOCK_GROUP
        assert GeoLevel.from_geoid_length(15) == GeoLevel.BLOCK

    def test_from_geoid_length_longer_values(self):
        """Test from_geoid_length with lengths longer than standard."""
        # Lengths >= 15 should return BLOCK
        assert GeoLevel.from_geoid_length(16) == GeoLevel.BLOCK
        assert GeoLevel.from_geoid_length(20) == GeoLevel.BLOCK

    def test_from_geoid_length_intermediate_values(self):
        """Test from_geoid_length with non-standard lengths."""
        # Length 3-4 should return STATE (less than 5 for county)
        assert GeoLevel.from_geoid_length(3) == GeoLevel.STATE
        assert GeoLevel.from_geoid_length(4) == GeoLevel.STATE
        # Length 6-10 should return COUNTY (>= 5 but < 11 for tract)
        assert GeoLevel.from_geoid_length(6) == GeoLevel.COUNTY
        assert GeoLevel.from_geoid_length(10) == GeoLevel.COUNTY
        # Length 13-14 should return BLOCK_GROUP (>= 12 but < 15)
        assert GeoLevel.from_geoid_length(13) == GeoLevel.BLOCK_GROUP
        assert GeoLevel.from_geoid_length(14) == GeoLevel.BLOCK_GROUP

    def test_from_geoid_length_short_values(self):
        """Test from_geoid_length with very short lengths."""
        assert GeoLevel.from_geoid_length(1) == GeoLevel.STATE
        assert GeoLevel.from_geoid_length(0) == GeoLevel.STATE


class TestGEOIDComponents:
    """Tests for GEOIDComponents dataclass."""

    def test_full_geoid_reconstruction(self):
        """Test that full_geoid reconstructs the GEOID."""
        components = GEOIDComponents(
            state="06",
            county="037",
            tract="101100",
            block_group="1",
            block="1001",
        )
        assert components.full_geoid == "060371011001001"

    def test_state_only(self):
        """Test with state only."""
        components = GEOIDComponents(state="06")
        assert components.full_geoid == "06"
        assert components.state_fips == "06"
        assert components.county_fips is None

    def test_county_fips_property(self):
        """Test county_fips property."""
        components = GEOIDComponents(state="06", county="037")
        assert components.county_fips == "06037"

    def test_tract_geoid_property(self):
        """Test tract_geoid property."""
        components = GEOIDComponents(state="06", county="037", tract="101100")
        assert components.tract_geoid == "06037101100"


class TestGEOIDParser:
    """Tests for GEOIDParser."""

    def test_parse_full_block_geoid(self):
        """Test parsing a full 15-digit block GEOID."""
        components = GEOIDParser.parse("060371011001001")

        assert components.state == "06"
        assert components.county == "037"
        assert components.tract == "101100"
        assert components.block_group == "1"
        assert components.block == "1001"

    def test_parse_tract_geoid(self):
        """Test parsing an 11-digit tract GEOID."""
        components = GEOIDParser.parse("06037101100")

        assert components.state == "06"
        assert components.county == "037"
        assert components.tract == "101100"
        assert components.block_group is None
        assert components.block is None

    def test_parse_county_geoid(self):
        """Test parsing a 5-digit county GEOID."""
        components = GEOIDParser.parse("06037")

        assert components.state == "06"
        assert components.county == "037"
        assert components.tract is None

    def test_parse_state_geoid(self):
        """Test parsing a 2-digit state GEOID."""
        components = GEOIDParser.parse("06")

        assert components.state == "06"
        assert components.county is None

    def test_parse_invalid_geoid_too_short(self):
        """Test that parsing fails for too-short GEOIDs."""
        with pytest.raises(ValueError, match="at least 2 digits"):
            GEOIDParser.parse("6")

    def test_parse_invalid_geoid_non_numeric(self):
        """Test that parsing fails for non-numeric GEOIDs."""
        with pytest.raises(ValueError, match="only digits"):
            GEOIDParser.parse("06ABC")

    def test_parse_empty_geoid(self):
        """Test that parsing fails for empty GEOIDs."""
        with pytest.raises(ValueError):
            GEOIDParser.parse("")

    def test_truncate_to_tract(self):
        """Test truncating a block GEOID to tract level."""
        geoid = "060371011001001"
        assert GEOIDParser.truncate(geoid, GeoLevel.TRACT) == "06037101100"

    def test_truncate_to_county(self):
        """Test truncating a block GEOID to county level."""
        geoid = "060371011001001"
        assert GEOIDParser.truncate(geoid, GeoLevel.COUNTY) == "06037"

    def test_truncate_to_state(self):
        """Test truncating a block GEOID to state level."""
        geoid = "060371011001001"
        assert GEOIDParser.truncate(geoid, GeoLevel.STATE) == "06"

    def test_get_parent(self):
        """Test getting parent GEOID."""
        geoid = "060371011001001"
        assert GEOIDParser.get_parent(geoid, GeoLevel.BLOCK_GROUP) == "060371011001"
        assert GEOIDParser.get_parent(geoid, GeoLevel.TRACT) == "06037101100"

    def test_get_level(self):
        """Test determining GEOID level from length."""
        assert GEOIDParser.get_level("060371011001001") == GeoLevel.BLOCK
        assert GEOIDParser.get_level("060371011001") == GeoLevel.BLOCK_GROUP
        assert GEOIDParser.get_level("06037101100") == GeoLevel.TRACT
        assert GEOIDParser.get_level("06037") == GeoLevel.COUNTY
        assert GEOIDParser.get_level("06") == GeoLevel.STATE

    def test_validate_valid_geoids(self):
        """Test validation of valid GEOIDs."""
        assert GEOIDParser.validate("060371011001001") is True
        assert GEOIDParser.validate("06037101100") is True
        assert GEOIDParser.validate("06037") is True
        assert GEOIDParser.validate("06") is True

    def test_validate_invalid_geoids(self):
        """Test validation of invalid GEOIDs."""
        assert GEOIDParser.validate("") is False
        assert GEOIDParser.validate("6") is False
        assert GEOIDParser.validate("06ABC") is False
        assert GEOIDParser.validate("060") is False  # Invalid length

    def test_validate_with_level(self):
        """Test validation with expected level."""
        assert GEOIDParser.validate("060371011001001", GeoLevel.BLOCK) is True
        assert GEOIDParser.validate("060371011001001", GeoLevel.TRACT) is False
        assert GEOIDParser.validate("06037101100", GeoLevel.TRACT) is True
