"""Tests for data catalog."""

import pytest
from pathlib import Path
from datetime import datetime

from census_lookup.data.catalog import DataCatalog, DatasetInfo


class TestDatasetInfo:
    """Tests for DatasetInfo dataclass."""

    def test_create_dataset_info(self, tmp_path):
        """Test creating DatasetInfo with factory method."""
        # Create a test file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test content")

        info = DatasetInfo.create(
            dataset_type="blocks",
            state_fips="06",
            file_path=test_file,
            source_url="https://example.com/blocks.zip",
        )

        assert info.dataset_type == "blocks"
        assert info.state_fips == "06"
        assert info.county_fips is None
        assert info.file_path == str(test_file)
        assert info.source_url == "https://example.com/blocks.zip"
        assert info.file_size > 0
        assert info.downloaded_at is not None


class TestDataCatalog:
    """Tests for DataCatalog."""

    @pytest.fixture
    def catalog(self, tmp_path):
        """Create a test catalog."""
        catalog_path = tmp_path / "catalog.json"
        return DataCatalog(catalog_path)

    @pytest.fixture
    def sample_info(self, tmp_path):
        """Create sample dataset info."""
        test_file = tmp_path / "blocks_06.parquet"
        test_file.write_text("test")
        return DatasetInfo.create(
            dataset_type="blocks",
            state_fips="06",
            file_path=test_file,
            source_url="https://example.com/blocks.zip",
        )

    def test_register_and_check_availability(self, catalog, sample_info):
        """Test registering a dataset and checking availability."""
        catalog.register(sample_info)

        assert catalog.is_available("blocks", "06") is True
        assert catalog.is_available("blocks", "07") is False
        assert catalog.is_available("addrfeat", "06") is False

    def test_get_info(self, catalog, sample_info):
        """Test retrieving dataset info."""
        catalog.register(sample_info)

        info = catalog.get_info("blocks", "06")

        assert info is not None
        assert info.dataset_type == "blocks"
        assert info.state_fips == "06"

    def test_get_path(self, catalog, sample_info):
        """Test getting path to dataset."""
        catalog.register(sample_info)

        path = catalog.get_path("blocks", "06")

        assert path is not None
        assert path.exists()

    def test_get_path_nonexistent(self, catalog):
        """Test getting path for nonexistent dataset."""
        path = catalog.get_path("blocks", "99")
        assert path is None

    def test_unregister(self, catalog, sample_info):
        """Test unregistering a dataset."""
        catalog.register(sample_info)
        assert catalog.is_available("blocks", "06") is True

        catalog.unregister("blocks", "06")
        assert catalog.is_available("blocks", "06") is False

    def test_list_states(self, catalog, tmp_path):
        """Test listing states with data."""
        # Register multiple states
        for state in ["06", "36", "48"]:
            test_file = tmp_path / f"blocks_{state}.parquet"
            test_file.write_text("test")
            info = DatasetInfo.create(
                dataset_type="blocks",
                state_fips=state,
                file_path=test_file,
                source_url="https://example.com",
            )
            catalog.register(info)

        states = catalog.list_states("blocks")

        assert len(states) == 3
        assert "06" in states
        assert "36" in states
        assert "48" in states

    def test_list_counties(self, catalog, tmp_path):
        """Test listing counties with data."""
        # Register county-level data
        for county in ["06001", "06037", "06075"]:
            test_file = tmp_path / f"addrfeat_{county}.parquet"
            test_file.write_text("test")
            info = DatasetInfo.create(
                dataset_type="addrfeat",
                state_fips="06",
                county_fips=county,
                file_path=test_file,
                source_url="https://example.com",
            )
            catalog.register(info)

        counties = catalog.list_counties("addrfeat", "06")

        assert len(counties) == 3
        assert "06001" in counties

    def test_get_total_size(self, catalog, sample_info, tmp_path):
        """Test getting total size."""
        catalog.register(sample_info)

        # Add another
        test_file = tmp_path / "blocks_36.parquet"
        test_file.write_text("more test content")
        info2 = DatasetInfo.create(
            dataset_type="blocks",
            state_fips="36",
            file_path=test_file,
            source_url="https://example.com",
        )
        catalog.register(info2)

        total = catalog.get_total_size()
        assert total > 0

    def test_get_size_by_type(self, catalog, tmp_path):
        """Test getting size grouped by type."""
        # Register blocks
        blocks_file = tmp_path / "blocks_06.parquet"
        blocks_file.write_text("blocks content")
        catalog.register(DatasetInfo.create(
            dataset_type="blocks",
            state_fips="06",
            file_path=blocks_file,
            source_url="https://example.com",
        ))

        # Register addrfeat
        addr_file = tmp_path / "addrfeat_06.parquet"
        addr_file.write_text("addr content")
        catalog.register(DatasetInfo.create(
            dataset_type="addrfeat",
            state_fips="06",
            file_path=addr_file,
            source_url="https://example.com",
        ))

        sizes = catalog.get_size_by_type()

        assert "blocks" in sizes
        assert "addrfeat" in sizes
        assert sizes["blocks"] > 0

    def test_persistence(self, tmp_path):
        """Test that catalog persists across instances."""
        catalog_path = tmp_path / "catalog.json"

        # Create and populate catalog
        catalog1 = DataCatalog(catalog_path)
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test")
        catalog1.register(DatasetInfo.create(
            dataset_type="blocks",
            state_fips="06",
            file_path=test_file,
            source_url="https://example.com",
        ))

        # Create new instance
        catalog2 = DataCatalog(catalog_path)

        assert catalog2.is_available("blocks", "06") is True

    def test_clear_all(self, catalog, sample_info):
        """Test clearing all entries."""
        catalog.register(sample_info)
        assert catalog.is_available("blocks", "06") is True

        catalog.clear()

        assert catalog.is_available("blocks", "06") is False
        assert len(catalog.list_states("blocks")) == 0

    def test_clear_by_state(self, catalog, tmp_path):
        """Test clearing entries for a specific state."""
        # Add multiple states
        for state in ["06", "36"]:
            test_file = tmp_path / f"blocks_{state}.parquet"
            test_file.write_text("test")
            catalog.register(DatasetInfo.create(
                dataset_type="blocks",
                state_fips=state,
                file_path=test_file,
                source_url="https://example.com",
            ))

        catalog.clear(state_fips="06")

        assert catalog.is_available("blocks", "06") is False
        assert catalog.is_available("blocks", "36") is True
