"""Type stubs for usaddress library."""

from typing import Dict, List, Tuple

class RepeatedLabelError(Exception):
    """Exception raised when address has repeated label tokens."""

    original_string: str
    parsed_string: List[Tuple[str, str]]

def tag(address: str) -> Tuple[Dict[str, str], str]: ...
def parse(address: str) -> List[Tuple[str, str]]: ...
