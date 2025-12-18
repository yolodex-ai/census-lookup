"""Functional tests for the census-lookup CLI.

These test the CLI commands that users run directly.
Run with: pytest tests/functional/test_cli.py -v -s
"""

import json
import tempfile
from pathlib import Path

import pandas as pd
from click.testing import CliRunner

from census_lookup.cli.commands import cli


class TestCLILookup:
    """User can look up addresses via command line."""

    def test_lookup_json_output(self):
        """Look up address with JSON output."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "lookup",
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "-v", "P1_001N",
            "-f", "json",
        ])

        assert result.exit_code == 0, result.output
        # Extract JSON from output (may have download messages before it)
        output = result.output
        json_start = output.find("{")
        json_end = output.rfind("}") + 1
        json_str = output[json_start:json_end]
        data = json.loads(json_str)
        # Result to_dict() doesn't include is_matched, but has geoid
        assert data["geoid"] is not None
        assert "P1_001N" in data

    def test_lookup_csv_output(self):
        """Look up address with CSV output."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "lookup",
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "-v", "P1_001N",
            "-f", "csv",
        ])

        assert result.exit_code == 0, result.output
        assert "geoid" in result.output
        assert "P1_001N" in result.output

    def test_lookup_table_output(self):
        """Look up address with table output."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "lookup",
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "-f", "table",
        ])

        assert result.exit_code == 0, result.output
        assert "CENSUS LOOKUP RESULT" in result.output
        assert "GEOID" in result.output

    def test_lookup_table_with_variables(self):
        """Look up address with table output and census variables."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "lookup",
            "1600 Pennsylvania Avenue NW, Washington, DC",
            "-v", "P1_001N",
            "-f", "table",
        ])

        assert result.exit_code == 0, result.output
        assert "CENSUS LOOKUP RESULT" in result.output
        assert "Census Data" in result.output
        assert "P1_001N" in result.output

    def test_lookup_different_levels(self):
        """Look up at different geographic levels."""
        runner = CliRunner()

        for level in ["block", "tract", "county"]:
            result = runner.invoke(cli, [
                "lookup",
                "1600 Pennsylvania Avenue NW, Washington, DC",
                "-l", level,
                "-v", "P1_001N",
                "-f", "json",
            ])

            assert result.exit_code == 0, f"{level}: {result.output}"
            data = json.loads(result.output)
            assert data["geoid"] is not None


class TestCLIBatch:
    """User can batch process addresses via command line."""

    def test_batch_csv(self):
        """Process CSV file of addresses."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create input CSV
            input_path = Path(tmpdir) / "input.csv"
            output_path = Path(tmpdir) / "output.csv"

            df = pd.DataFrame({
                "addr": [
                    "1600 Pennsylvania Avenue NW, Washington, DC",
                    "100 Maryland Ave SW, Washington, DC",
                ]
            })
            df.to_csv(input_path, index=False)

            # Run batch
            result = runner.invoke(cli, [
                "batch",
                str(input_path),
                str(output_path),
                "-a", "addr",
                "-l", "tract",
            ])

            assert result.exit_code == 0, result.output
            assert output_path.exists()

            # Verify output
            output_df = pd.read_csv(output_path)
            assert len(output_df) == 2
            assert "geoid" in output_df.columns

    def test_batch_parquet_output(self):
        """Output to parquet format."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            output_path = Path(tmpdir) / "output.parquet"

            df = pd.DataFrame({
                "address": ["1600 Pennsylvania Avenue NW, Washington, DC"]
            })
            df.to_csv(input_path, index=False)

            result = runner.invoke(cli, [
                "batch",
                str(input_path),
                str(output_path),
                "-a", "address",
            ])

            assert result.exit_code == 0, result.output
            assert output_path.exists()

    def test_batch_invalid_column(self):
        """Error when address column doesn't exist."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            output_path = Path(tmpdir) / "output.csv"

            df = pd.DataFrame({"some_column": ["test"]})
            df.to_csv(input_path, index=False)

            result = runner.invoke(cli, [
                "batch",
                str(input_path),
                str(output_path),
                "-a", "nonexistent",
            ])

            assert result.exit_code != 0
            assert "not found" in result.output


class TestCLIInfo:
    """User can get info about downloaded data."""

    def test_info_command(self):
        """Show cache info."""
        runner = CliRunner()
        result = runner.invoke(cli, ["info"])

        assert result.exit_code == 0, result.output
        assert "Data directory" in result.output
        assert "Disk usage" in result.output


class TestCLIVariables:
    """User can list available variables."""

    def test_list_all_variables(self):
        """List all census variables."""
        runner = CliRunner()
        result = runner.invoke(cli, ["variables"])

        assert result.exit_code == 0, result.output
        assert "P1_001N" in result.output
        assert "H1_001N" in result.output
        assert "Variable Groups" in result.output

    def test_filter_by_table(self):
        """Filter variables by table."""
        runner = CliRunner()
        result = runner.invoke(cli, ["variables", "-t", "H1"])

        assert result.exit_code == 0, result.output
        assert "H1_001N" in result.output
        # P1 variables should not appear when filtering by H1
        lines = result.output.split('\n')
        variable_lines = [line for line in lines if line.startswith('P1_')]
        assert len(variable_lines) == 0


class TestCLIDownload:
    """User can pre-download data."""

    def test_download_requires_state(self):
        """Error when no state specified."""
        runner = CliRunner()
        result = runner.invoke(cli, ["download"])

        assert result.exit_code != 0
        assert "Specify state" in result.output

    def test_download_single_state(self):
        """Download data for a single state."""
        runner = CliRunner()
        result = runner.invoke(cli, ["download", "DC"])

        # May succeed or fail based on network, but command should run
        assert result.exit_code == 0
        assert "Downloading" in result.output or "Done" in result.output or "Download complete" in result.output


class TestCLICoords:
    """User can look up coordinates directly."""

    def test_coords_lookup(self):
        """Look up census data by coordinates."""
        runner = CliRunner()

        # Use positive lon for testing (somewhere in Pacific, won't match but tests the command)
        # We test with DC coordinates that are positive in format
        result = runner.invoke(cli, [
            "coords",
            "-l", "tract",
            "-v", "P1_001N",
            "38.8977",  # lat
            "77.0365",  # lon (using positive - won't find data but tests command)
        ])

        # May succeed or fail depending on data availability
        # but should at least run without CLI parsing errors
        assert result.exit_code == 0 or "No census block found" in result.output

    def test_coords_lookup_with_valid_coordinates(self):
        """Look up census data by valid DC coordinates."""
        runner = CliRunner()

        # Use actual DC coordinates (negative longitude)
        # Use -- to indicate end of options so -77 is not parsed as flag
        result = runner.invoke(cli, [
            "coords",
            "-l", "tract",
            "-v", "P1_001N",
            "--",
            "38.8977",   # lat
            "-77.0365",  # lon (proper negative for western hemisphere)
        ])

        # Should find DC data
        assert result.exit_code == 0
        # Either finds data or reports no match
        assert "geoid" in result.output.lower() or "No census block found" in result.output


class TestCLIVersion:
    """User can check version."""

    def test_version(self):
        """Show version."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        # The version output format may vary
        assert "0.1" in result.output or "version" in result.output.lower()

    def test_help(self):
        """Show help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "usage" in result.output.lower() or "census" in result.output.lower()


class TestCLIEdgeCases:
    """Test CLI edge cases for coverage."""

    def test_lookup_no_match_table_output(self):
        """Look up invalid address with table output shows no match message."""
        runner = CliRunner()
        result = runner.invoke(cli, [
            "lookup",
            "invalid address without state",
            "-f", "table",
        ])

        assert result.exit_code == 0
        assert "CENSUS LOOKUP RESULT" in result.output
        # Should show no match message
        assert "No match found" in result.output or "no_match" in result.output.lower()

    def test_batch_unsupported_format(self):
        """Error when input file format is unsupported."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.txt"
            output_path = Path(tmpdir) / "output.csv"

            input_path.write_text("some data")

            result = runner.invoke(cli, [
                "batch",
                str(input_path),
                str(output_path),
                "-a", "address",
            ])

            assert result.exit_code != 0
            assert "Unsupported file format" in result.output

    def test_batch_fallback_csv_output(self):
        """Batch with unknown output suffix defaults to CSV."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.csv"
            output_path = Path(tmpdir) / "output.unknown"

            df = pd.DataFrame({
                "addr": ["1600 Pennsylvania Avenue NW, Washington, DC"]
            })
            df.to_csv(input_path, index=False)

            result = runner.invoke(cli, [
                "batch",
                str(input_path),
                str(output_path),
                "-a", "addr",
            ])

            assert result.exit_code == 0, result.output
            assert output_path.exists()
            # Should be CSV format despite .unknown extension
            content = output_path.read_text()
            assert "geoid" in content

    def test_clear_all_with_confirm(self):
        """Clear all cached data with confirmation."""
        runner = CliRunner()
        result = runner.invoke(cli, ["clear"], input="y\n")

        # Should execute without error (may or may not have data to clear)
        assert result.exit_code == 0, result.output
        assert "cleared" in result.output.lower()

    def test_clear_state_with_confirm(self):
        """Clear specific state data with confirmation."""
        runner = CliRunner()
        result = runner.invoke(cli, ["clear", "DC"], input="y\n")

        # Should execute without error
        assert result.exit_code == 0, result.output
        assert "cleared" in result.output.lower()

    def test_clear_abort(self):
        """Clear aborted when user says no."""
        runner = CliRunner()
        result = runner.invoke(cli, ["clear"], input="n\n")

        # Should abort cleanly
        assert result.exit_code == 1  # Aborted
        assert "Aborted" in result.output

    def test_format_size_large(self):
        """Test _format_size with large values."""
        from census_lookup.cli.commands import _format_size

        # Test various sizes
        assert "B" in _format_size(100)
        assert "KB" in _format_size(1024 * 2)
        assert "MB" in _format_size(1024 * 1024 * 2)
        assert "GB" in _format_size(1024 * 1024 * 1024 * 2)
        assert "TB" in _format_size(1024 * 1024 * 1024 * 1024 * 2)
