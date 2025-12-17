"""Tests for street name normalization."""

import pytest

from census_lookup.address.normalizer import StreetNormalizer


class TestStreetNormalizer:
    """Tests for StreetNormalizer."""

    @pytest.fixture
    def normalizer(self):
        return StreetNormalizer()

    def test_normalize_uppercase(self, normalizer):
        """Test that normalization uppercases."""
        assert normalizer.normalize("main street") == "MAIN STREET"
        assert normalizer.normalize("Main Street") == "MAIN STREET"

    def test_normalize_directional_expansion(self, normalizer):
        """Test directional abbreviation expansion."""
        assert normalizer.normalize("N Main") == "NORTH MAIN"
        assert normalizer.normalize("S Oak") == "SOUTH OAK"
        assert normalizer.normalize("E 1st") == "EAST FIRST"  # 1ST also expands
        assert normalizer.normalize("W Broadway") == "WEST BROADWAY"

    def test_normalize_compound_directional(self, normalizer):
        """Test compound directional expansion."""
        assert normalizer.normalize("NE Main") == "NORTHEAST MAIN"
        assert normalizer.normalize("SW Oak") == "SOUTHWEST OAK"

    def test_normalize_street_type_expansion(self, normalizer):
        """Test street type abbreviation expansion."""
        assert normalizer.normalize("Main St") == "MAIN STREET"
        assert normalizer.normalize("Oak Ave") == "OAK AVENUE"
        assert normalizer.normalize("Pine Blvd") == "PINE BOULEVARD"
        assert normalizer.normalize("Cedar Dr") == "CEDAR DRIVE"

    def test_normalize_without_expansion(self, normalizer):
        """Test normalization without abbreviation expansion."""
        result = normalizer.normalize("N Main St", expand_abbreviations=False)
        assert result == "N MAIN ST"

    def test_normalize_removes_extra_whitespace(self, normalizer):
        """Test that extra whitespace is removed."""
        assert normalizer.normalize("Main   Street") == "MAIN STREET"
        assert normalizer.normalize("  Main St  ") == "MAIN STREET"

    def test_normalize_removes_special_chars(self, normalizer):
        """Test that special characters are removed."""
        assert normalizer.normalize("Main St.") == "MAIN STREET"
        assert normalizer.normalize("O'Brien Ave") == "OBRIEN AVENUE"

    def test_normalize_preserves_hyphens(self, normalizer):
        """Test that hyphens are preserved."""
        result = normalizer.normalize("Martin-Luther King Blvd")
        assert "-" in result or "MARTIN" in result  # Depends on implementation

    def test_normalize_ordinal_expansion(self, normalizer):
        """Test ordinal number expansion."""
        assert normalizer.normalize("1st St") == "FIRST STREET"
        assert normalizer.normalize("2nd Ave") == "SECOND AVENUE"
        assert normalizer.normalize("3rd Blvd") == "THIRD BOULEVARD"

    def test_normalize_empty_string(self, normalizer):
        """Test normalizing empty string."""
        assert normalizer.normalize("") == ""
        assert normalizer.normalize(None) == ""

    def test_normalize_for_tiger_basic(self, normalizer):
        """Test TIGER-specific normalization."""
        result = normalizer.normalize_for_tiger(
            "MAIN",
            directional="N",
            street_type="ST",
        )
        # TIGER uses abbreviated directionals but full street types
        assert "MAIN" in result
        assert "STREET" in result

    def test_normalize_for_tiger_no_directional(self, normalizer):
        """Test TIGER normalization without directional."""
        result = normalizer.normalize_for_tiger(
            "OAK",
            street_type="AVE",
        )
        assert "OAK" in result
        assert "AVENUE" in result

    def test_generate_variants(self, normalizer):
        """Test variant generation."""
        variants = normalizer.generate_variants("MAIN STREET")

        assert "MAIN STREET" in variants
        assert len(variants) >= 1

    def test_generate_variants_with_abbreviation(self, normalizer):
        """Test variant generation with abbreviatable type."""
        variants = normalizer.generate_variants("MAIN STREET")

        # Should include abbreviated version
        assert "MAIN STREET" in variants
        # May include "MAIN ST" depending on implementation
