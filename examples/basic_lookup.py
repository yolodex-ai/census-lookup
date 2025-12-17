"""Basic example of using census-lookup."""

from census_lookup import CensusLookup, GeoLevel

# Initialize the lookup
# - geo_level: Geographic level for results (BLOCK, BLOCK_GROUP, TRACT, COUNTY)
# - variables: Census variables to include in results
# - auto_download: Automatically download data for states as needed
lookup = CensusLookup(
    geo_level=GeoLevel.BLOCK,
    variables=["P1_001N", "P1_003N", "P1_004N", "H1_001N"],
    auto_download=True,
)

# Single address lookup
print("=" * 60)
print("Single Address Lookup")
print("=" * 60)

address = "1600 Pennsylvania Avenue NW, Washington, DC 20500"
result = lookup.geocode(address)

if result.is_matched:
    print(f"Address: {address}")
    print(f"Matched: {result.matched_address}")
    print(f"Location: ({result.latitude:.6f}, {result.longitude:.6f})")
    print(f"GEOID: {result.geoid}")
    print(f"State FIPS: {result.state_fips}")
    print(f"County FIPS: {result.county_fips}")
    print(f"Tract: {result.tract}")
    print("\nCensus Data:")
    for var, value in result.census_data.items():
        print(f"  {var}: {value}")
else:
    print(f"Could not geocode: {address}")
    print(f"Match type: {result.match_type}")

# Coordinate lookup (if you already have lat/lon)
print("\n" + "=" * 60)
print("Coordinate Lookup")
print("=" * 60)

# Note: Coordinate lookup requires the relevant state data to be loaded
lat, lon = 38.8977, -77.0365  # White House coordinates
result = lookup.lookup_coordinates(lat, lon, geo_level=GeoLevel.TRACT)

if result.is_matched:
    print(f"Coordinates: ({lat}, {lon})")
    print(f"Tract GEOID: {result.geoid}")
    print(f"Census Data: {result.census_data}")
else:
    print("No census block found for coordinates")
    print("Make sure the relevant state data is loaded")
