"""Tests for FIPS code constants and normalization."""

import pytest

from census_lookup.data.constants import (
    FIPS_STATES,
    STATE_ABBREVS,
    normalize_state,
    get_state_name,
    get_state_abbrev,
)


class TestFIPSConstants:
    """Tests for FIPS code constants."""

    def test_all_states_present(self):
        """Test that all 50 states plus DC and PR are present."""
        assert len(FIPS_STATES) >= 52  # 50 states + DC + PR

    def test_california_fips(self):
        """Test California FIPS code."""
        assert "06" in FIPS_STATES
        assert FIPS_STATES["06"] == "California"

    def test_new_york_fips(self):
        """Test New York FIPS code."""
        assert "36" in FIPS_STATES
        assert FIPS_STATES["36"] == "New York"

    def test_texas_fips(self):
        """Test Texas FIPS code."""
        assert "48" in FIPS_STATES
        assert FIPS_STATES["48"] == "Texas"

    def test_dc_fips(self):
        """Test District of Columbia FIPS code."""
        assert "11" in FIPS_STATES
        assert FIPS_STATES["11"] == "District of Columbia"


class TestStateAbbrevs:
    """Tests for state abbreviation mapping."""

    def test_california_abbrev(self):
        """Test California abbreviation."""
        assert STATE_ABBREVS["CA"] == "06"

    def test_new_york_abbrev(self):
        """Test New York abbreviation."""
        assert STATE_ABBREVS["NY"] == "36"

    def test_all_abbrevs_map_to_valid_fips(self):
        """Test that all abbreviations map to valid FIPS codes."""
        for abbrev, fips in STATE_ABBREVS.items():
            assert fips in FIPS_STATES, f"{abbrev} maps to invalid FIPS {fips}"


class TestNormalizeState:
    """Tests for normalize_state function."""

    def test_normalize_fips_code(self):
        """Test normalizing a FIPS code."""
        assert normalize_state("06") == "06"
        assert normalize_state("36") == "36"

    def test_normalize_abbreviation(self):
        """Test normalizing a state abbreviation."""
        assert normalize_state("CA") == "06"
        assert normalize_state("NY") == "36"
        assert normalize_state("TX") == "48"

    def test_normalize_abbreviation_lowercase(self):
        """Test normalizing lowercase abbreviation."""
        assert normalize_state("ca") == "06"
        assert normalize_state("ny") == "36"

    def test_normalize_full_name(self):
        """Test normalizing a full state name."""
        assert normalize_state("California") == "06"
        assert normalize_state("New York") == "36"

    def test_normalize_full_name_lowercase(self):
        """Test normalizing lowercase state name."""
        assert normalize_state("california") == "06"
        assert normalize_state("new york") == "36"

    def test_normalize_with_whitespace(self):
        """Test normalizing with surrounding whitespace."""
        assert normalize_state("  CA  ") == "06"
        assert normalize_state(" California ") == "06"

    def test_normalize_invalid_fips(self):
        """Test that invalid FIPS code raises error."""
        with pytest.raises(ValueError, match="Unknown state"):
            normalize_state("99")

    def test_normalize_invalid_abbrev(self):
        """Test that invalid abbreviation raises error."""
        with pytest.raises(ValueError, match="Unknown state"):
            normalize_state("XX")

    def test_normalize_invalid_name(self):
        """Test that invalid name raises error."""
        with pytest.raises(ValueError, match="Unknown state"):
            normalize_state("NotAState")


class TestGetStateName:
    """Tests for get_state_name function."""

    def test_get_california_name(self):
        """Test getting California name."""
        assert get_state_name("06") == "California"

    def test_get_unknown_fips_name(self):
        """Test getting name for unknown FIPS."""
        result = get_state_name("99")
        assert "99" in result  # Should include the code in the result


class TestGetStateAbbrev:
    """Tests for get_state_abbrev function."""

    def test_get_california_abbrev(self):
        """Test getting California abbreviation."""
        assert get_state_abbrev("06") == "CA"

    def test_get_new_york_abbrev(self):
        """Test getting New York abbreviation."""
        assert get_state_abbrev("36") == "NY"

    def test_get_unknown_fips_abbrev(self):
        """Test getting abbreviation for unknown FIPS."""
        result = get_state_abbrev("99")
        assert result == "99"  # Should return the code itself
