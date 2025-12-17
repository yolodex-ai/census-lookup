"""Example of batch processing addresses with census-lookup."""

import pandas as pd
from census_lookup import CensusLookup, GeoLevel

# Create sample data
addresses = [
    "100 Main St, Los Angeles, CA 90012",
    "200 Broadway, New York, NY 10007",
    "300 Market St, San Francisco, CA 94105",
    "400 Congress Ave, Austin, TX 78701",
    "Invalid Address That Won't Match",
]

df = pd.DataFrame({"address": addresses, "id": range(1, len(addresses) + 1)})

print("Input DataFrame:")
print(df)
print()

# Initialize lookup with multiple variable groups
lookup = CensusLookup(
    geo_level=GeoLevel.TRACT,  # Aggregate to tract level
    variable_groups=["population", "housing"],
    auto_download=True,
)

# Batch geocode
print("Processing addresses...")
results = lookup.geocode_batch(df["address"], progress=True)

# Combine original data with results
output = pd.concat([df, results], axis=1)

print("\nResults:")
print(output[["id", "address", "geoid", "match_type", "P1_001N", "H1_001N"]])

# Summary statistics
matched = results["match_type"].isin(["interpolated", "exact"]).sum()
print(f"\nMatch rate: {matched}/{len(df)} ({100*matched/len(df):.1f}%)")

# Save to file
# output.to_csv("geocoded_addresses.csv", index=False)
# output.to_parquet("geocoded_addresses.parquet")
