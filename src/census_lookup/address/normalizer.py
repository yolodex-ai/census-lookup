"""Street name normalization for TIGER matching."""

import re
from typing import Dict


class StreetNormalizer:
    """
    Normalize street names for matching against TIGER data.

    Handles:
    - Directional abbreviations (N -> NORTH)
    - Street type variations (ST -> STREET, AVE -> AVENUE)
    - Ordinal numbers (1ST -> FIRST)
    - Case normalization
    - Special characters
    """

    # Directional abbreviations to full form
    DIRECTIONALS: Dict[str, str] = {
        "N": "NORTH",
        "S": "SOUTH",
        "E": "EAST",
        "W": "WEST",
        "NE": "NORTHEAST",
        "NW": "NORTHWEST",
        "SE": "SOUTHEAST",
        "SW": "SOUTHWEST",
        "NO": "NORTH",
        "SO": "SOUTH",
    }

    # Reverse mapping: full form to abbreviation (for TIGER matching)
    DIRECTIONALS_ABBREV: Dict[str, str] = {
        "NORTH": "N",
        "SOUTH": "S",
        "EAST": "E",
        "WEST": "W",
        "NORTHEAST": "NE",
        "NORTHWEST": "NW",
        "SOUTHEAST": "SE",
        "SOUTHWEST": "SW",
    }

    # Street type abbreviations to full form
    STREET_TYPES: Dict[str, str] = {
        "ST": "STREET",
        "STR": "STREET",
        "AVE": "AVENUE",
        "AV": "AVENUE",
        "BLVD": "BOULEVARD",
        "BLV": "BOULEVARD",
        "DR": "DRIVE",
        "DRV": "DRIVE",
        "RD": "ROAD",
        "LN": "LANE",
        "CT": "COURT",
        "CRT": "COURT",
        "PL": "PLACE",
        "WAY": "WAY",
        "CIR": "CIRCLE",
        "CRCL": "CIRCLE",
        "TRL": "TRAIL",
        "TR": "TRAIL",
        "PKWY": "PARKWAY",
        "PKY": "PARKWAY",
        "HWY": "HIGHWAY",
        "HWAY": "HIGHWAY",
        "EXPY": "EXPRESSWAY",
        "EXP": "EXPRESSWAY",
        "EXPW": "EXPRESSWAY",
        "FWY": "FREEWAY",
        "FRWY": "FREEWAY",
        "ALY": "ALLEY",
        "ALLY": "ALLEY",
        "ANX": "ANNEX",
        "ARC": "ARCADE",
        "BCH": "BEACH",
        "BND": "BEND",
        "BRG": "BRIDGE",
        "BRK": "BROOK",
        "BYP": "BYPASS",
        "CYN": "CANYON",
        "CPE": "CAPE",
        "CSWY": "CAUSEWAY",
        "CTR": "CENTER",
        "CLF": "CLIFF",
        "CLB": "CLUB",
        "CMN": "COMMON",
        "CMNS": "COMMONS",
        "CRK": "CREEK",
        "CRES": "CRESCENT",
        "CRST": "CREST",
        "XING": "CROSSING",
        "DL": "DALE",
        "DM": "DAM",
        "DV": "DIVIDE",
        "EST": "ESTATE",
        "ESTS": "ESTATES",
        "FALL": "FALL",
        "FLS": "FALLS",
        "FRY": "FERRY",
        "FLD": "FIELD",
        "FLDS": "FIELDS",
        "FLT": "FLAT",
        "FLTS": "FLATS",
        "FRD": "FORD",
        "FRST": "FOREST",
        "FRG": "FORGE",
        "FRK": "FORK",
        "FRKS": "FORKS",
        "FT": "FORT",
        "GDN": "GARDEN",
        "GDNS": "GARDENS",
        "GTWY": "GATEWAY",
        "GLN": "GLEN",
        "GRN": "GREEN",
        "GRV": "GROVE",
        "HBR": "HARBOR",
        "HVN": "HAVEN",
        "HTS": "HEIGHTS",
        "HL": "HILL",
        "HLS": "HILLS",
        "HOLW": "HOLLOW",
        "INLT": "INLET",
        "IS": "ISLAND",
        "ISS": "ISLANDS",
        "JCT": "JUNCTION",
        "KY": "KEY",
        "KYS": "KEYS",
        "KNL": "KNOLL",
        "KNLS": "KNOLLS",
        "LK": "LAKE",
        "LKS": "LAKES",
        "LNDG": "LANDING",
        "LGT": "LIGHT",
        "LF": "LOAF",
        "LCK": "LOCK",
        "LCKS": "LOCKS",
        "LDG": "LODGE",
        "LOOP": "LOOP",
        "MALL": "MALL",
        "MNR": "MANOR",
        "MDWS": "MEADOWS",
        "ML": "MILL",
        "MLS": "MILLS",
        "MSN": "MISSION",
        "MT": "MOUNT",
        "MTN": "MOUNTAIN",
        "NCK": "NECK",
        "ORCH": "ORCHARD",
        "OVAL": "OVAL",
        "PARK": "PARK",
        "PASS": "PASS",
        "PATH": "PATH",
        "PIKE": "PIKE",
        "PNE": "PINE",
        "PNES": "PINES",
        "PLN": "PLAIN",
        "PLNS": "PLAINS",
        "PLZ": "PLAZA",
        "PT": "POINT",
        "PTS": "POINTS",
        "PRT": "PORT",
        "PRTS": "PORTS",
        "PR": "PRAIRIE",
        "RADL": "RADIAL",
        "RNCH": "RANCH",
        "RPD": "RAPID",
        "RPDS": "RAPIDS",
        "RST": "REST",
        "RDG": "RIDGE",
        "RDGS": "RIDGES",
        "RIV": "RIVER",
        "ROW": "ROW",
        "RUN": "RUN",
        "SHL": "SHOAL",
        "SHLS": "SHOALS",
        "SHR": "SHORE",
        "SHRS": "SHORES",
        "SPG": "SPRING",
        "SPGS": "SPRINGS",
        "SPUR": "SPUR",
        "SQ": "SQUARE",
        "SQS": "SQUARES",
        "STA": "STATION",
        "STRA": "STRAVENUE",
        "STRM": "STREAM",
        "SMT": "SUMMIT",
        "TER": "TERRACE",
        "TRCE": "TRACE",
        "TRAK": "TRACK",
        "TRFY": "TRAFFICWAY",
        "TUNL": "TUNNEL",
        "TPKE": "TURNPIKE",
        "UN": "UNION",
        "UNS": "UNIONS",
        "VLY": "VALLEY",
        "VLYS": "VALLEYS",
        "VIA": "VIADUCT",
        "VW": "VIEW",
        "VWS": "VIEWS",
        "VLG": "VILLAGE",
        "VLGS": "VILLAGES",
        "VL": "VILLE",
        "VIS": "VISTA",
        "WALK": "WALK",
        "WALL": "WALL",
        "WL": "WELL",
        "WLS": "WELLS",
    }

    # Reverse mapping: full form to preferred TIGER abbreviation
    # Note: TIGER data uses specific abbreviations, we select the most common ones
    STREET_TYPES_ABBREV: Dict[str, str] = {
        "STREET": "ST",
        "AVENUE": "AVE",
        "BOULEVARD": "BLVD",
        "DRIVE": "DR",
        "ROAD": "RD",
        "LANE": "LN",
        "COURT": "CT",
        "PLACE": "PL",
        "WAY": "WAY",
        "CIRCLE": "CIR",
        "TRAIL": "TRL",
        "PARKWAY": "PKWY",
        "HIGHWAY": "HWY",
        "EXPRESSWAY": "EXPY",
        "FREEWAY": "FWY",
        "ALLEY": "ALY",
        "ANNEX": "ANX",
        "ARCADE": "ARC",
        "BEACH": "BCH",
        "BEND": "BND",
        "BRIDGE": "BRG",
        "BROOK": "BRK",
        "BYPASS": "BYP",
        "CANYON": "CYN",
        "CAPE": "CPE",
        "CAUSEWAY": "CSWY",
        "CENTER": "CTR",
        "CLIFF": "CLF",
        "CLUB": "CLB",
        "COMMON": "CMN",
        "COMMONS": "CMNS",
        "CREEK": "CRK",
        "CRESCENT": "CRES",
        "CREST": "CRST",
        "CROSSING": "XING",
        "DALE": "DL",
        "DAM": "DM",
        "DIVIDE": "DV",
        "ESTATE": "EST",
        "ESTATES": "ESTS",
        "FALLS": "FLS",
        "FERRY": "FRY",
        "FIELD": "FLD",
        "FIELDS": "FLDS",
        "FLAT": "FLT",
        "FLATS": "FLTS",
        "FORD": "FRD",
        "FOREST": "FRST",
        "FORGE": "FRG",
        "FORK": "FRK",
        "FORKS": "FRKS",
        "FORT": "FT",
        "GARDEN": "GDN",
        "GARDENS": "GDNS",
        "GATEWAY": "GTWY",
        "GLEN": "GLN",
        "GREEN": "GRN",
        "GROVE": "GRV",
        "HARBOR": "HBR",
        "HAVEN": "HVN",
        "HEIGHTS": "HTS",
        "HILL": "HL",
        "HILLS": "HLS",
        "HOLLOW": "HOLW",
        "INLET": "INLT",
        "ISLAND": "IS",
        "ISLANDS": "ISS",
        "JUNCTION": "JCT",
        "KEY": "KY",
        "KEYS": "KYS",
        "KNOLL": "KNL",
        "KNOLLS": "KNLS",
        "LAKE": "LK",
        "LAKES": "LKS",
        "LANDING": "LNDG",
        "LIGHT": "LGT",
        "LOAF": "LF",
        "LOCK": "LCK",
        "LOCKS": "LCKS",
        "LODGE": "LDG",
        "LOOP": "LOOP",
        "MALL": "MALL",
        "MANOR": "MNR",
        "MEADOWS": "MDWS",
        "MILL": "ML",
        "MILLS": "MLS",
        "MISSION": "MSN",
        "MOUNT": "MT",
        "MOUNTAIN": "MTN",
        "NECK": "NCK",
        "ORCHARD": "ORCH",
        "OVAL": "OVAL",
        "PARK": "PARK",
        "PASS": "PASS",
        "PATH": "PATH",
        "PIKE": "PIKE",
        "PINE": "PNE",
        "PINES": "PNES",
        "PLAIN": "PLN",
        "PLAINS": "PLNS",
        "PLAZA": "PLZ",
        "POINT": "PT",
        "POINTS": "PTS",
        "PORT": "PRT",
        "PORTS": "PRTS",
        "PRAIRIE": "PR",
        "RADIAL": "RADL",
        "RANCH": "RNCH",
        "RAPID": "RPD",
        "RAPIDS": "RPDS",
        "REST": "RST",
        "RIDGE": "RDG",
        "RIDGES": "RDGS",
        "RIVER": "RIV",
        "ROW": "ROW",
        "RUN": "RUN",
        "SHOAL": "SHL",
        "SHOALS": "SHLS",
        "SHORE": "SHR",
        "SHORES": "SHRS",
        "SPRING": "SPG",
        "SPRINGS": "SPGS",
        "SPUR": "SPUR",
        "SQUARE": "SQ",
        "SQUARES": "SQS",
        "STATION": "STA",
        "STRAVENUE": "STRA",
        "STREAM": "STRM",
        "SUMMIT": "SMT",
        "TERRACE": "TER",
        "TRACE": "TRCE",
        "TRACK": "TRAK",
        "TRAFFICWAY": "TRFY",
        "TUNNEL": "TUNL",
        "TURNPIKE": "TPKE",
        "UNION": "UN",
        "UNIONS": "UNS",
        "VALLEY": "VLY",
        "VALLEYS": "VLYS",
        "VIADUCT": "VIA",
        "VIEW": "VW",
        "VIEWS": "VWS",
        "VILLAGE": "VLG",
        "VILLAGES": "VLGS",
        "VILLE": "VL",
        "VISTA": "VIS",
        "WALK": "WALK",
        "WALL": "WALL",
        "WELL": "WL",
        "WELLS": "WLS",
    }

    # Ordinal numbers
    ORDINALS: Dict[str, str] = {
        "1ST": "FIRST",
        "2ND": "SECOND",
        "3RD": "THIRD",
        "4TH": "FOURTH",
        "5TH": "FIFTH",
        "6TH": "SIXTH",
        "7TH": "SEVENTH",
        "8TH": "EIGHTH",
        "9TH": "NINTH",
        "10TH": "TENTH",
        "11TH": "ELEVENTH",
        "12TH": "TWELFTH",
    }

    def normalize(self, street_name: str) -> str:
        """
        Normalize a street name for matching.

        Args:
            street_name: Street name to normalize

        Returns:
            Normalized uppercase street name
        """
        if not street_name:
            return ""

        # Uppercase
        result = street_name.upper().strip()

        # Remove extra whitespace
        result = " ".join(result.split())

        # Remove special characters except spaces and hyphens
        result = re.sub(r"[^\w\s\-]", "", result)

        return result

    def generate_variants(self, street_name: str) -> list[str]:
        """
        Generate possible variants of a street name for fuzzy matching.

        Args:
            street_name: Normalized street name

        Returns:
            List of variant strings to try matching
        """
        variants = [street_name]

        words = street_name.split()

        # Try converting full street types to abbreviations (AVENUE -> AVE)
        # This is important for matching TIGER data which uses abbreviations
        new_words = []
        changed = False
        for word in words:
            if word in self.STREET_TYPES_ABBREV:
                # For common types, prefer the first (canonical) abbreviation
                abbrev = self.STREET_TYPES_ABBREV[word]
                new_words.append(abbrev)
                changed = True
            else:
                new_words.append(word)
        if changed:
            variants.append(" ".join(new_words))

        # Try with abbreviated directionals (NORTHWEST -> NW)
        new_words = []
        changed = False
        for word in words:
            if word in self.DIRECTIONALS_ABBREV:
                new_words.append(self.DIRECTIONALS_ABBREV[word])
                changed = True
            else:
                new_words.append(word)
        if changed:
            variants.append(" ".join(new_words))

        # Try both street type AND directional abbreviations together
        new_words = []
        for word in words:
            if word in self.STREET_TYPES_ABBREV:
                new_words.append(self.STREET_TYPES_ABBREV[word])
            elif word in self.DIRECTIONALS_ABBREV:
                new_words.append(self.DIRECTIONALS_ABBREV[word])
            else:
                new_words.append(word)
        combined = " ".join(new_words)
        if combined not in variants:
            variants.append(combined)

        # Try without street type (last word if it's a type)
        if len(words) > 1:
            last_word = words[-1]
            if last_word in self.STREET_TYPES_ABBREV or last_word in self.STREET_TYPES:
                variants.append(" ".join(words[:-1]))

        return variants
