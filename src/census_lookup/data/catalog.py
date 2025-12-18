"""Data catalog for tracking downloaded datasets."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class DatasetInfo:
    """Metadata about a downloaded dataset."""

    dataset_type: str  # "blocks", "addrfeat", "pl94171", etc.
    state_fips: str
    file_path: str
    downloaded_at: str  # ISO format datetime
    file_size: int  # bytes
    source_url: str
    checksum: Optional[str] = None

    @classmethod
    def create(
        cls,
        dataset_type: str,
        state_fips: str,
        file_path: Path,
        source_url: str,
    ) -> "DatasetInfo":
        """Create a new DatasetInfo with current timestamp."""
        return cls(
            dataset_type=dataset_type,
            state_fips=state_fips,
            file_path=str(file_path),
            downloaded_at=datetime.now().isoformat(),
            file_size=file_path.stat().st_size if file_path.exists() else 0,
            source_url=source_url,
        )


@dataclass
class CatalogData:
    """Root catalog data structure."""

    version: str = "1.0"
    datasets: Dict[str, DatasetInfo] = field(default_factory=dict)


class DataCatalog:
    """
    Tracks downloaded and converted data.

    Persists to catalog.json for cross-session awareness.
    """

    def __init__(self, catalog_path: Path):
        """
        Initialize catalog.

        Args:
            catalog_path: Path to catalog.json file
        """
        self.catalog_path = catalog_path
        self._data = CatalogData()
        self._load()

    def _load(self) -> None:
        """Load catalog from disk."""
        if self.catalog_path.exists():
            try:
                with open(self.catalog_path, "r") as f:
                    raw = json.load(f)
                    self._data.version = raw.get("version", "1.0")
                    datasets = raw.get("datasets", {})
                    self._data.datasets = {k: DatasetInfo(**v) for k, v in datasets.items()}
            except (json.JSONDecodeError, TypeError):
                # Corrupted catalog, start fresh
                self._data = CatalogData()

    def _save(self) -> None:
        """Persist catalog to disk."""
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.catalog_path, "w") as f:
            data = {
                "version": self._data.version,
                "datasets": {k: asdict(v) for k, v in self._data.datasets.items()},
            }
            json.dump(data, f, indent=2)

    def _make_key(self, dataset_type: str, state_fips: str) -> str:
        """Create a unique key for a dataset."""
        return f"{dataset_type}:{state_fips}"

    def register(self, info: DatasetInfo) -> None:
        """
        Register a downloaded dataset.

        Args:
            info: Dataset metadata
        """
        key = self._make_key(info.dataset_type, info.state_fips)
        self._data.datasets[key] = info
        self._save()

    def unregister(self, dataset_type: str, state_fips: str) -> None:
        """Remove a dataset from the catalog."""
        key = self._make_key(dataset_type, state_fips)
        if key in self._data.datasets:
            del self._data.datasets[key]
            self._save()

    def is_available(self, dataset_type: str, state_fips: str) -> bool:
        """
        Check if a dataset is available locally.

        Args:
            dataset_type: Type of dataset
            state_fips: State FIPS code

        Returns:
            True if dataset is registered and file exists
        """
        key = self._make_key(dataset_type, state_fips)
        if key not in self._data.datasets:
            return False

        # Verify file still exists
        info = self._data.datasets[key]
        path = Path(info.file_path)
        return path.exists()

    def get_info(self, dataset_type: str, state_fips: str) -> Optional[DatasetInfo]:
        """Get metadata for a dataset if available."""
        key = self._make_key(dataset_type, state_fips)
        return self._data.datasets.get(key)

    def get_path(self, dataset_type: str, state_fips: str) -> Optional[Path]:
        """
        Get path to a dataset if available.

        Reloads catalog from disk if entry not found (handles concurrent updates).

        Returns:
            Path to dataset file, or None if not available
        """
        info = self.get_info(dataset_type, state_fips)
        if not info:
            # Reload from disk in case another process/instance updated it
            self._load()
            info = self.get_info(dataset_type, state_fips)
        if info:
            path = Path(info.file_path)
            # is_available() checks existence before get_path is called
            assert path.exists(), f"File {path} registered but missing"
            return path
        return None

    def list_states(self, dataset_type: str) -> List[str]:
        """
        List states with available data for a dataset type.

        Args:
            dataset_type: Type of dataset

        Returns:
            List of state FIPS codes
        """
        states = set()
        for _, info in self._data.datasets.items():
            if info.dataset_type == dataset_type:
                states.add(info.state_fips)
        return sorted(states)

    def clear(self) -> None:
        """Clear all catalog entries."""
        self._data.datasets.clear()

        self._save()
