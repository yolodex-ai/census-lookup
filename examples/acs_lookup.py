"""Example of looking up ACS (American Community Survey) data.

ACS provides richer demographic data than PL 94-171 redistricting data,
including income, education, employment, housing characteristics, and more.

Note: ACS data is available at tract level and above (not block level).
"""

from census_lookup import (
    CensusLookup,
    GeoLevel,
    get_acs_variables_for_group,
    list_acs_variable_groups,
)

# Show available ACS variable groups
print("=" * 60)
print("Available ACS Variable Groups")
print("=" * 60)
for group, description in list_acs_variable_groups().items():
    print(f"  {group:20s} - {description}")
print()

# Initialize with ACS variables (income, education)
# You can specify variables explicitly or use variable groups
lookup = CensusLookup(
    geo_level=GeoLevel.TRACT,  # ACS data is at tract level
    variables=["P1_001N"],  # PL 94-171 population (for comparison)
    acs_variables=[
        "B19013_001E",  # Median household income
        "B15003_022E",  # Bachelor's degree
        "B25077_001E",  # Median home value
    ],
    # OR use variable groups:
    # acs_variable_groups=["income", "education"],
    auto_download=True,
)

# Single address lookup
print("=" * 60)
print("Single Address Lookup (with ACS data)")
print("=" * 60)

address = "1600 Pennsylvania Avenue NW, Washington, DC 20500"
result = lookup.geocode(address)

if result.is_matched:
    print(f"Address: {address}")
    print(f"Matched: {result.matched_address}")
    print(f"Tract GEOID: {result.tract}")
    print()
    print("Census Data:")
    for var, value in result.census_data.items():
        # Format currency values nicely
        if var.startswith("B19") or var == "B25077_001E":
            if value is not None:
                print(f"  {var}: ${value:,.0f}")
            else:
                print(f"  {var}: N/A")
        else:
            print(f"  {var}: {value}")
else:
    print(f"Could not geocode: {address}")

# Example 2: Using variable groups
print("\n" + "=" * 60)
print("Using Variable Groups")
print("=" * 60)

# Get all variables in the 'income' group
income_vars = get_acs_variables_for_group("income")
print(f"Income group has {len(income_vars)} variables:")
for var in income_vars[:5]:  # Show first 5
    print(f"  - {var}")

# You can dynamically add variable groups
lookup.add_acs_variable_group("poverty")
print(f"\nAfter adding poverty group, now tracking {len(lookup.acs_variables)} ACS variables")

# Example 3: Comparing PL 94-171 vs ACS
print("\n" + "=" * 60)
print("Data Source Comparison")
print("=" * 60)

print("""
PL 94-171 (Redistricting Data):
  - Available at: Block level and above
  - Variables: Race, Hispanic origin, voting age, housing units
  - Use for: Redistricting analysis, demographic counts

ACS 5-Year Estimates:
  - Available at: Tract level and above (NOT block level)
  - Variables: Income, education, employment, housing, health, etc.
  - Use for: Socioeconomic analysis, community profiles

When you request both PL 94-171 and ACS variables:
  - PL 94-171 data is joined at your requested geo_level
  - ACS data is joined at tract level (or your requested level if tract or higher)
""")
