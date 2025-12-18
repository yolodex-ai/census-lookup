"""American Community Survey (ACS) 5-Year Estimates variable definitions.

ACS provides richer demographic and socioeconomic data beyond PL 94-171,
including income, education, employment, housing characteristics, and more.

Reference: https://www.census.gov/data/developers/data-sets/acs-5year.html
"""

from typing import Dict, List

# ACS 5-Year Variable Definitions (2020)
# Format: Table_ColumnE for estimates, Table_ColumnM for margins of error
# Reference: https://api.census.gov/data/2020/acs/acs5/variables.html

ACS_VARIABLES: Dict[str, str] = {
    # Demographics (B01)
    "B01001_001E": "Total Population",
    "B01001_002E": "Male Population",
    "B01001_026E": "Female Population",
    "B01002_001E": "Median Age",
    "B01003_001E": "Total Population (alternate)",
    # Age Distribution (B01001)
    "B01001_003E": "Male Under 5 years",
    "B01001_004E": "Male 5 to 9 years",
    "B01001_005E": "Male 10 to 14 years",
    "B01001_006E": "Male 15 to 17 years",
    "B01001_007E": "Male 18 and 19 years",
    "B01001_008E": "Male 20 years",
    "B01001_009E": "Male 21 years",
    "B01001_010E": "Male 22 to 24 years",
    "B01001_011E": "Male 25 to 29 years",
    "B01001_012E": "Male 30 to 34 years",
    "B01001_013E": "Male 35 to 39 years",
    "B01001_014E": "Male 40 to 44 years",
    "B01001_015E": "Male 45 to 49 years",
    "B01001_016E": "Male 50 to 54 years",
    "B01001_017E": "Male 55 to 59 years",
    "B01001_018E": "Male 60 and 61 years",
    "B01001_019E": "Male 62 to 64 years",
    "B01001_020E": "Male 65 and 66 years",
    "B01001_021E": "Male 67 to 69 years",
    "B01001_022E": "Male 70 to 74 years",
    "B01001_023E": "Male 75 to 79 years",
    "B01001_024E": "Male 80 to 84 years",
    "B01001_025E": "Male 85 years and over",
    # Income (B19)
    "B19013_001E": "Median Household Income",
    "B19001_001E": "Households by Income - Total",
    "B19001_002E": "Households Income Less than $10,000",
    "B19001_003E": "Households Income $10,000 to $14,999",
    "B19001_004E": "Households Income $15,000 to $19,999",
    "B19001_005E": "Households Income $20,000 to $24,999",
    "B19001_006E": "Households Income $25,000 to $29,999",
    "B19001_007E": "Households Income $30,000 to $34,999",
    "B19001_008E": "Households Income $35,000 to $39,999",
    "B19001_009E": "Households Income $40,000 to $44,999",
    "B19001_010E": "Households Income $45,000 to $49,999",
    "B19001_011E": "Households Income $50,000 to $59,999",
    "B19001_012E": "Households Income $60,000 to $74,999",
    "B19001_013E": "Households Income $75,000 to $99,999",
    "B19001_014E": "Households Income $100,000 to $124,999",
    "B19001_015E": "Households Income $125,000 to $149,999",
    "B19001_016E": "Households Income $150,000 to $199,999",
    "B19001_017E": "Households Income $200,000 or more",
    "B19301_001E": "Per Capita Income",
    "B19083_001E": "Gini Index of Income Inequality",
    # Poverty (B17)
    "B17001_001E": "Poverty Status - Total Population",
    "B17001_002E": "Below Poverty Level",
    "B17001_031E": "At or Above Poverty Level",
    "B17020_001E": "Poverty Status by Age - Total",
    "B17020_002E": "Poverty Status Under 6 years",
    # Education (B15)
    "B15003_001E": "Educational Attainment - Population 25+",
    "B15003_002E": "No schooling completed",
    "B15003_003E": "Nursery school",
    "B15003_004E": "Kindergarten",
    "B15003_005E": "1st grade",
    "B15003_006E": "2nd grade",
    "B15003_007E": "3rd grade",
    "B15003_008E": "4th grade",
    "B15003_009E": "5th grade",
    "B15003_010E": "6th grade",
    "B15003_011E": "7th grade",
    "B15003_012E": "8th grade",
    "B15003_013E": "9th grade",
    "B15003_014E": "10th grade",
    "B15003_015E": "11th grade",
    "B15003_016E": "12th grade, no diploma",
    "B15003_017E": "High school diploma",
    "B15003_018E": "GED or alternative credential",
    "B15003_019E": "Some college, less than 1 year",
    "B15003_020E": "Some college, 1 or more years, no degree",
    "B15003_021E": "Associate's degree",
    "B15003_022E": "Bachelor's degree",
    "B15003_023E": "Master's degree",
    "B15003_024E": "Professional school degree",
    "B15003_025E": "Doctorate degree",
    # Employment (B23)
    "B23025_001E": "Employment Status - Population 16+",
    "B23025_002E": "In Labor Force",
    "B23025_003E": "Civilian Labor Force",
    "B23025_004E": "Employed",
    "B23025_005E": "Unemployed",
    "B23025_006E": "Armed Forces",
    "B23025_007E": "Not in Labor Force",
    # Occupation (C24)
    "C24010_001E": "Occupation - Civilian employed 16+",
    "C24010_002E": "Male Civilian employed 16+",
    "C24010_003E": "Male Management, business, science, arts",
    "C24010_038E": "Female Civilian employed 16+",
    "C24010_039E": "Female Management, business, science, arts",
    # Industry (C24)
    "C24030_001E": "Industry - Civilian employed 16+",
    "C24030_002E": "Agriculture, forestry, fishing, hunting, mining",
    "C24030_003E": "Construction",
    "C24030_004E": "Manufacturing",
    "C24030_005E": "Wholesale trade",
    "C24030_006E": "Retail trade",
    "C24030_007E": "Transportation, warehousing, utilities",
    "C24030_008E": "Information",
    "C24030_009E": "Finance, insurance, real estate",
    "C24030_010E": "Professional, scientific, management, admin, waste",
    "C24030_011E": "Educational services, health care, social assistance",
    "C24030_012E": "Arts, entertainment, recreation, food services",
    "C24030_013E": "Other services",
    "C24030_014E": "Public administration",
    # Commute (B08)
    "B08301_001E": "Means of Transportation to Work - Total",
    "B08301_002E": "Car, truck, or van",
    "B08301_003E": "Car, truck, van - drove alone",
    "B08301_004E": "Car, truck, van - carpooled",
    "B08301_010E": "Public transportation",
    "B08301_016E": "Taxicab",
    "B08301_017E": "Motorcycle",
    "B08301_018E": "Bicycle",
    "B08301_019E": "Walked",
    "B08301_020E": "Other means",
    "B08301_021E": "Worked from home",
    "B08303_001E": "Travel Time to Work - Total",
    "B08013_001E": "Aggregate Travel Time to Work (minutes)",
    # Housing (B25)
    "B25001_001E": "Total Housing Units",
    "B25002_001E": "Occupancy Status - Total",
    "B25002_002E": "Occupied Housing Units",
    "B25002_003E": "Vacant Housing Units",
    "B25003_001E": "Tenure - Total Occupied Units",
    "B25003_002E": "Owner-Occupied",
    "B25003_003E": "Renter-Occupied",
    "B25024_001E": "Units in Structure - Total",
    "B25024_002E": "1 unit, detached",
    "B25024_003E": "1 unit, attached",
    "B25024_004E": "2 units",
    "B25024_005E": "3 or 4 units",
    "B25024_006E": "5 to 9 units",
    "B25024_007E": "10 to 19 units",
    "B25024_008E": "20 to 49 units",
    "B25024_009E": "50 or more units",
    "B25024_010E": "Mobile home",
    "B25024_011E": "Boat, RV, van, etc.",
    "B25035_001E": "Median Year Structure Built",
    "B25064_001E": "Median Gross Rent",
    "B25077_001E": "Median Home Value",
    "B25071_001E": "Median Gross Rent as % of Household Income",
    "B25070_001E": "Gross Rent as % of Income - Total",
    "B25070_010E": "Gross Rent 50% or more of income",
    # Health Insurance (B27)
    "B27001_001E": "Health Insurance Coverage - Total",
    "B27001_004E": "Under 6 with health insurance",
    "B27001_005E": "Under 6 without health insurance",
    "B27010_001E": "Health Insurance by Type - Total",
    "B27010_017E": "Employer-based insurance",
    "B27010_033E": "Direct-purchase insurance",
    "B27010_050E": "Medicare",
    "B27010_066E": "Medicaid/means-tested public coverage",
    # Household Composition (B11)
    "B11001_001E": "Households - Total",
    "B11001_002E": "Family households",
    "B11001_003E": "Married-couple family",
    "B11001_004E": "Male householder, no spouse",
    "B11001_005E": "Female householder, no spouse",
    "B11001_007E": "Nonfamily households",
    "B11001_008E": "Householder living alone",
    "B11016_001E": "Household Type by Size - Total",
    "B25010_001E": "Average Household Size",
    # Language (B16)
    "B16001_001E": "Language Spoken at Home - Population 5+",
    "B16001_002E": "Speak only English",
    "B16001_003E": "Speak Spanish",
    "B16001_006E": "Speak French, Haitian, or Cajun",
    "B16001_009E": "Speak German or West Germanic",
    "B16001_012E": "Speak Russian, Polish, or other Slavic",
    "B16001_015E": "Speak other Indo-European",
    "B16001_018E": "Speak Korean",
    "B16001_021E": "Speak Chinese (incl. Mandarin, Cantonese)",
    "B16001_024E": "Speak Vietnamese",
    "B16001_027E": "Speak Tagalog",
    "B16001_030E": "Speak other Asian/Pacific Islander",
    "B16001_033E": "Speak Arabic",
    "B16001_036E": "Speak other/unspecified language",
    # Nativity and Citizenship (B05)
    "B05001_001E": "Nativity and Citizenship - Total",
    "B05001_002E": "U.S. citizen, born in US",
    "B05001_003E": "U.S. citizen, born in PR or Island Areas",
    "B05001_004E": "U.S. citizen, born abroad of American parents",
    "B05001_005E": "U.S. citizen by naturalization",
    "B05001_006E": "Not a U.S. citizen",
    # Internet Access (B28)
    "B28002_001E": "Internet Access - Total Households",
    "B28002_002E": "With an Internet subscription",
    "B28002_003E": "Dial-up with no other type",
    "B28002_004E": "Broadband of any type",
    "B28002_007E": "Cellular data plan",
    "B28002_012E": "Without Internet subscription",
    "B28002_013E": "No Internet access",
    "B28003_001E": "Computer in Household - Total",
    "B28003_002E": "Has a computer",
    "B28003_004E": "No computer",
    # Vehicles Available (B25044)
    "B25044_001E": "Vehicles Available - Occupied Housing Units",
    "B25044_003E": "Owner-occupied - No vehicle",
    "B25044_004E": "Owner-occupied - 1 vehicle",
    "B25044_005E": "Owner-occupied - 2 vehicles",
    "B25044_006E": "Owner-occupied - 3 vehicles",
    "B25044_007E": "Owner-occupied - 4 vehicles",
    "B25044_008E": "Owner-occupied - 5+ vehicles",
    "B25044_010E": "Renter-occupied - No vehicle",
    "B25044_011E": "Renter-occupied - 1 vehicle",
}

# Common variable groups for convenience
ACS_VARIABLE_GROUPS: Dict[str, List[str]] = {
    "demographics": [
        "B01001_001E",  # Total population
        "B01001_002E",  # Male
        "B01001_026E",  # Female
        "B01002_001E",  # Median age
    ],
    "income": [
        "B19013_001E",  # Median household income
        "B19301_001E",  # Per capita income
        "B19083_001E",  # Gini index
    ],
    "income_distribution": [f"B19001_{i:03d}E" for i in range(1, 18)],
    "poverty": [
        "B17001_001E",  # Total
        "B17001_002E",  # Below poverty
        "B17001_031E",  # At or above poverty
    ],
    "education": [
        "B15003_001E",  # Total 25+
        "B15003_017E",  # HS diploma
        "B15003_021E",  # Associate's
        "B15003_022E",  # Bachelor's
        "B15003_023E",  # Master's
        "B15003_024E",  # Professional
        "B15003_025E",  # Doctorate
    ],
    "education_detailed": [f"B15003_{i:03d}E" for i in range(1, 26)],
    "employment": [
        "B23025_001E",  # Total 16+
        "B23025_002E",  # In labor force
        "B23025_004E",  # Employed
        "B23025_005E",  # Unemployed
        "B23025_007E",  # Not in labor force
    ],
    "commute": [
        "B08301_001E",  # Total workers
        "B08301_003E",  # Drove alone
        "B08301_004E",  # Carpooled
        "B08301_010E",  # Public transit
        "B08301_019E",  # Walked
        "B08301_021E",  # Worked from home
    ],
    "housing": [
        "B25001_001E",  # Total units
        "B25002_002E",  # Occupied
        "B25002_003E",  # Vacant
        "B25003_002E",  # Owner-occupied
        "B25003_003E",  # Renter-occupied
        "B25077_001E",  # Median home value
        "B25064_001E",  # Median gross rent
    ],
    "housing_detailed": [
        "B25001_001E",
        "B25002_002E",
        "B25002_003E",
        "B25003_002E",
        "B25003_003E",
        "B25077_001E",
        "B25064_001E",
        "B25035_001E",  # Median year built
        "B25071_001E",  # Rent burden
    ]
    + [f"B25024_{i:03d}E" for i in range(2, 12)],  # Units in structure
    "health_insurance": [
        "B27001_001E",  # Total
        "B27010_017E",  # Employer-based
        "B27010_033E",  # Direct-purchase
        "B27010_050E",  # Medicare
        "B27010_066E",  # Medicaid
    ],
    "household": [
        "B11001_001E",  # Total households
        "B11001_002E",  # Family
        "B11001_003E",  # Married-couple
        "B11001_007E",  # Nonfamily
        "B25010_001E",  # Avg household size
    ],
    "language": [
        "B16001_001E",  # Total 5+
        "B16001_002E",  # English only
        "B16001_003E",  # Spanish
        "B16001_021E",  # Chinese
    ],
    "citizenship": [
        "B05001_001E",  # Total
        "B05001_002E",  # Born in US
        "B05001_005E",  # Naturalized
        "B05001_006E",  # Not citizen
    ],
    "internet": [
        "B28002_001E",  # Total households
        "B28002_004E",  # Broadband
        "B28002_007E",  # Cellular
        "B28002_013E",  # No internet
    ],
    "vehicles": [
        "B25044_001E",  # Total occupied
        "B25044_003E",  # Owner - no vehicle
        "B25044_010E",  # Renter - no vehicle
    ],
}

# Default ACS variables (commonly used subset)
DEFAULT_ACS_VARIABLES: List[str] = [
    "B01001_001E",  # Total population
    "B01002_001E",  # Median age
    "B19013_001E",  # Median household income
    "B19301_001E",  # Per capita income
    "B17001_002E",  # Below poverty level
    "B15003_022E",  # Bachelor's degree
    "B23025_004E",  # Employed
    "B23025_005E",  # Unemployed
    "B25001_001E",  # Total housing units
    "B25077_001E",  # Median home value
    "B25064_001E",  # Median gross rent
    "B25003_002E",  # Owner-occupied
    "B25003_003E",  # Renter-occupied
    "B28002_004E",  # Broadband internet
]


def get_acs_variables_for_group(group: str) -> List[str]:
    """
    Get list of ACS variables for a named group.

    Args:
        group: Group name (e.g., "income", "education", "housing")

    Returns:
        List of variable codes

    Raises:
        ValueError: If group name is not recognized
    """
    if group not in ACS_VARIABLE_GROUPS:
        valid = ", ".join(ACS_VARIABLE_GROUPS.keys())
        raise ValueError(f"Unknown ACS variable group: {group}. Valid groups: {valid}")

    return ACS_VARIABLE_GROUPS[group]


def list_acs_tables() -> Dict[str, str]:
    """List available ACS table groups."""
    return {
        "B01": "Sex and Age",
        "B05": "Nativity and Citizenship",
        "B08": "Commuting/Transportation",
        "B11": "Household Type and Relationships",
        "B15": "Educational Attainment",
        "B16": "Language Spoken at Home",
        "B17": "Poverty Status",
        "B19": "Income",
        "B23": "Employment Status",
        "B25": "Housing Characteristics",
        "B27": "Health Insurance",
        "B28": "Internet Access and Computers",
        "C24": "Industry and Occupation",
    }


def list_acs_variable_groups() -> Dict[str, str]:
    """List available ACS variable groups with descriptions."""
    return {
        "demographics": "Age, sex, median age",
        "income": "Household income, per capita income, Gini index",
        "income_distribution": "Household income brackets",
        "poverty": "Poverty status",
        "education": "Educational attainment (common levels)",
        "education_detailed": "Educational attainment (all levels)",
        "employment": "Employment status and labor force",
        "commute": "Means of transportation to work",
        "housing": "Housing occupancy, tenure, values",
        "housing_detailed": "Housing characteristics (extended)",
        "health_insurance": "Health insurance coverage by type",
        "household": "Household composition and size",
        "language": "Language spoken at home",
        "citizenship": "Nativity and citizenship status",
        "internet": "Internet access and computer ownership",
        "vehicles": "Vehicles available per household",
    }
