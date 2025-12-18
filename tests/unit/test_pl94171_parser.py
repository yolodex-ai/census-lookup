"""Tests for PL 94-171 parser edge cases."""

import io
import zipfile
from pathlib import Path

import pytest

from census_lookup.data.pl94171_parser import parse_pl94171_zip


class TestPL94171ParserEdgeCases:
    """Test PL 94-171 parser edge cases."""

    def test_malformed_zip_no_geo_file_raises_error(self, tmp_path: Path):
        """When zip file has no geo file, raises ValueError."""
        # Create a zip file without the geo file
        zip_path = tmp_path / "malformed.zip"

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # Add some random file but NOT the geo file
            zf.writestr("random_file.txt", "some content")

        zip_path.write_bytes(buffer.getvalue())

        with pytest.raises(ValueError, match="No geo file found"):
            parse_pl94171_zip(zip_path)

    def test_parse_with_variables_none_returns_all_columns(self, tmp_path: Path):
        """When variables=None, all data columns are returned (minus headers)."""
        # Create a minimal valid PL 94-171 zip file
        zip_path = tmp_path / "dc2020.pl.zip"

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            # Geo file - SUMLEV at position 2, LOGRECNO at 7, GEOID at 9
            # Summary level 750 = block
            geo_parts = [""] * 2 + ["750"] + [""] * 4 + ["0000001"]
            geo_parts += [""] + ["7500000US110010001011000"] + [""] * 10
            geo_content = "|".join(geo_parts)
            zf.writestr("dcgeo2020.pl", geo_content)

            # Segment 1 - FILEID, STUSAB, CHAESSION, CIESSION, LOGRECNO,
            # then P1_001N-P1_071N, P2_001N-P2_073N
            # Total: 5 header + 71 P1 + 73 P2 = 149 columns
            seg1_values = ["PL94171", "DC", "000", "01", "0000001"]
            seg1_values += ["100"] * 71  # P1 columns
            seg1_values += ["200"] * 73  # P2 columns
            zf.writestr("dc000012020.pl", "|".join(seg1_values))

            # Segment 2 - 5 header + 71 P3 + 73 P4 + 3 H1 = 152 columns
            seg2_values = ["PL94171", "DC", "000", "01", "0000001"]
            seg2_values += ["300"] * 71  # P3 columns
            seg2_values += ["400"] * 73  # P4 columns
            seg2_values += ["500", "501", "502"]  # H1 columns
            zf.writestr("dc000022020.pl", "|".join(seg2_values))

        zip_path.write_bytes(buffer.getvalue())

        # Parse with variables=None to get all columns
        result = parse_pl94171_zip(zip_path, variables=None, summary_level="750")

        # Should have GEOID and all P1, P2, P3, P4, H1 columns
        assert "GEOID" in result.columns
        assert "P1_001N" in result.columns
        assert "P2_001N" in result.columns
        assert "P3_001N" in result.columns
        assert "P4_001N" in result.columns
        assert "H1_001N" in result.columns

        # Header columns should be dropped
        assert "LOGRECNO" not in result.columns
        assert "FILEID" not in result.columns
        assert "STUSAB" not in result.columns

    def test_parse_with_specific_variables_filters_columns(self, tmp_path: Path):
        """When variables list is provided, only those columns are returned."""
        zip_path = tmp_path / "dc2020.pl.zip"

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            geo_parts = [""] * 2 + ["750"] + [""] * 4 + ["0000001"]
            geo_parts += [""] + ["7500000US110010001011000"] + [""] * 10
            geo_content = "|".join(geo_parts)
            zf.writestr("dcgeo2020.pl", geo_content)

            seg1_values = ["PL94171", "DC", "000", "01", "0000001"]
            seg1_values += ["100"] * 71
            seg1_values += ["200"] * 73
            zf.writestr("dc000012020.pl", "|".join(seg1_values))

            seg2_values = ["PL94171", "DC", "000", "01", "0000001"]
            seg2_values += ["300"] * 71
            seg2_values += ["400"] * 73
            seg2_values += ["500", "501", "502"]
            zf.writestr("dc000022020.pl", "|".join(seg2_values))

        zip_path.write_bytes(buffer.getvalue())

        # Parse with specific variables
        result = parse_pl94171_zip(zip_path, variables=["P1_001N", "H1_001N"], summary_level="750")

        # Should have GEOID and only requested variables
        assert list(result.columns) == ["GEOID", "P1_001N", "H1_001N"]
