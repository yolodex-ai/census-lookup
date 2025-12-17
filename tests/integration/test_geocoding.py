"""Integration tests for geocoding functionality.

These tests use real addresses and download actual Census data.
They test the full pipeline from address parsing to census data retrieval.

Run with: pytest tests/integration -v -s
"""

import pytest

from census_lookup import CensusLookup, GeoLevel


# Use DC for tests - it's the smallest state (~15MB) and downloads quickly
STATE_FIPS_DC = "11"


@pytest.fixture(scope="module")
def lookup_block():
    """Create a CensusLookup instance at block level for DC."""
    return CensusLookup(
        geo_level=GeoLevel.BLOCK,
        variables=["P1_001N", "H1_001N"],
        auto_download=True,
    )


@pytest.fixture(scope="module")
def lookup_tract():
    """Create a CensusLookup instance at tract level for DC."""
    return CensusLookup(
        geo_level=GeoLevel.TRACT,
        variables=["P1_001N"],
        auto_download=True,
    )


class TestSingleAddressGeocoding:
    """Test single address geocoding at various levels."""

    def test_white_house_block_level(self, lookup_block):
        """Test geocoding the White House at block level."""
        result = lookup_block.geocode(
            "1600 Pennsylvania Avenue NW, Washington, DC 20500"
        )

        assert result.is_matched
        assert result.state_fips == "11"  # DC
        assert result.county_fips == "11001"  # DC only has one county
        assert result.geoid.startswith("11001")
        assert len(result.geoid) == 15  # Block-level GEOID

        # Should have coordinates near the White House
        assert 38.89 < result.latitude < 38.91
        assert -77.04 < result.longitude < -77.02

        # Should have census data
        assert result.census_data is not None
        assert "P1_001N" in result.census_data
        assert "H1_001N" in result.census_data

    def test_white_house_tract_level(self, lookup_tract):
        """Test geocoding the White House at tract level with aggregated data."""
        result = lookup_tract.geocode(
            "1600 Pennsylvania Avenue NW, Washington, DC 20500"
        )

        assert result.is_matched
        assert result.geoid == "11001010100"  # Tract GEOID (11 digits)
        assert len(result.geoid) == 11

        # Tract should have aggregated population
        assert result.census_data is not None
        pop = result.census_data.get("P1_001N")
        assert pop is not None
        # White House tract should have a few thousand people
        assert 1000 < pop < 10000

    def test_capitol_building(self, lookup_block):
        """Test geocoding the US Capitol Building."""
        result = lookup_block.geocode(
            "First St SE, Washington, DC 20004"
        )

        assert result.is_matched
        assert result.state_fips == "11"
        # Capitol is in a different tract than White House
        assert result.tract is not None

    def test_address_with_apartment(self, lookup_block):
        """Test geocoding an address with apartment number."""
        result = lookup_block.geocode(
            "1301 U St NW Apt 305, Washington, DC 20009"
        )

        # Should still match even with apartment number
        assert result.is_matched
        assert result.state_fips == "11"


class TestAddressVariations:
    """Test different address format variations."""

    def test_abbreviated_street_type(self, lookup_block):
        """Test address with abbreviated street type (St vs Street)."""
        result = lookup_block.geocode("1600 Pennsylvania Ave NW, Washington, DC")
        assert result.is_matched

    def test_full_street_type(self, lookup_block):
        """Test address with full street type."""
        result = lookup_block.geocode(
            "1600 Pennsylvania Avenue Northwest, Washington, DC"
        )
        assert result.is_matched

    def test_lowercase_address(self, lookup_block):
        """Test lowercase address."""
        result = lookup_block.geocode(
            "1600 pennsylvania avenue nw, washington, dc"
        )
        assert result.is_matched

    def test_no_zip_code(self, lookup_block):
        """Test address without zip code."""
        result = lookup_block.geocode("1600 Pennsylvania Ave NW, Washington, DC")
        assert result.is_matched

    def test_zip_code_only(self, lookup_block):
        """Test address with just city and zip."""
        result = lookup_block.geocode("1600 Pennsylvania Ave NW, DC 20500")
        assert result.is_matched


class TestUnmatchedAddresses:
    """Test handling of addresses that can't be matched."""

    def test_invalid_address(self, lookup_block):
        """Test with an invalid/nonexistent address."""
        result = lookup_block.geocode("99999 Nonexistent Street, Washington, DC")

        # Should return a result but not matched
        assert not result.is_matched
        assert result.match_type in ["no_match", "parse_error"]

    def test_po_box(self, lookup_block):
        """Test PO Box address (can't be geocoded to a physical location)."""
        result = lookup_block.geocode("PO Box 12345, Washington, DC 20005")

        # PO Boxes typically can't be matched to a physical block
        # The behavior may vary - either no match or a centroid match
        # Just verify it doesn't crash
        assert result is not None


class TestGeographicLevels:
    """Test different geographic level queries."""

    def test_block_group_level(self):
        """Test geocoding at block group level."""
        lookup = CensusLookup(
            geo_level=GeoLevel.BLOCK_GROUP,
            variables=["P1_001N"],
            auto_download=True,
        )
        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert len(result.geoid) == 12  # Block group is 12 digits

    def test_county_level(self):
        """Test geocoding at county level."""
        lookup = CensusLookup(
            geo_level=GeoLevel.COUNTY,
            variables=["P1_001N"],
            auto_download=True,
        )
        result = lookup.geocode("1600 Pennsylvania Avenue NW, Washington, DC")

        assert result.is_matched
        assert result.geoid == "11001"  # DC county FIPS
        assert len(result.geoid) == 5

        # County-level population should be much higher (all of DC)
        pop = result.census_data.get("P1_001N")
        assert pop is not None
        assert pop > 600000  # DC has ~700k people


class TestBatchGeocoding:
    """Test batch geocoding functionality."""

    def test_batch_multiple_addresses(self, lookup_tract):
        """Test batch geocoding multiple DC addresses."""
        addresses = [
            "1600 Pennsylvania Avenue NW, Washington, DC",  # White House
            "2 15th St NW, Washington, DC",  # Near Washington Monument
            "100 Maryland Ave SW, Washington, DC",  # Near Capitol
        ]

        results = lookup_tract.geocode_batch(addresses)

        assert len(results) == 3
        assert "geoid" in results.columns
        assert "P1_001N" in results.columns

        # All should be in DC
        assert all(results["state_fips"] == "11")

        # At least some should be matched
        matched = results["match_type"].isin(["interpolated", "exact"]).sum()
        assert matched >= 2  # At least 2 of 3 should match

    def test_batch_with_some_failures(self, lookup_tract):
        """Test batch with some addresses that won't match."""
        addresses = [
            "1600 Pennsylvania Avenue NW, Washington, DC",  # Valid
            "99999 Fake Street, Washington, DC",  # Invalid
            "100 Maryland Ave SW, Washington, DC",  # Valid
        ]

        results = lookup_tract.geocode_batch(addresses)

        assert len(results) == 3

        # Check that we got results for all, even the invalid one
        valid_matches = results["match_type"].isin(["interpolated", "exact"]).sum()
        assert valid_matches >= 2


class TestCensusDataAccuracy:
    """Test that census data values are reasonable."""

    def test_population_is_reasonable(self, lookup_tract):
        """Verify tract population is within expected range."""
        result = lookup_tract.geocode(
            "1600 Pennsylvania Avenue NW, Washington, DC"
        )

        pop = result.census_data.get("P1_001N")

        # Census tracts typically have 1,200-8,000 people
        # Some can be smaller or larger, but this range catches most
        assert 100 < pop < 20000

    def test_housing_units_reasonable(self, lookup_block):
        """Verify housing unit count is reasonable."""
        result = lookup_block.geocode(
            "1600 Pennsylvania Avenue NW, Washington, DC"
        )

        housing = result.census_data.get("H1_001N")

        # Block-level housing units should be a positive number
        # The White House block might have few units
        assert housing is not None
        assert housing >= 0


class TestCoordinateLookup:
    """Test direct coordinate-based lookups."""

    def test_coordinates_white_house(self, lookup_tract):
        """Test looking up census data by coordinates."""
        # White House coordinates
        result = lookup_tract.lookup_coordinates(
            lat=38.8977,
            lon=-77.0365,
        )

        assert result.is_matched
        assert result.state_fips == "11"
        assert result.census_data is not None
