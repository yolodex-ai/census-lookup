"""Download TIGER/Line and Census data files."""

import shutil
import zipfile
from pathlib import Path
from typing import Callable, List, Optional
from urllib.parse import urlparse

import requests
from tqdm import tqdm

from census_lookup.data.constants import TIGER_URLS


class DownloadError(Exception):
    """Error downloading a file."""

    def __init__(self, url: str, status_code: int, message: str = ""):
        self.url = url
        self.status_code = status_code
        super().__init__(f"Failed to download {url}: HTTP {status_code}. {message}")


class TIGERDownloader:
    """
    Downloads TIGER/Line shapefiles from Census Bureau.

    Data sources:
    - Address Features: https://www2.census.gov/geo/tiger/TIGER2020/ADDRFEAT/
    - Blocks: https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/
    - Block Groups: https://www2.census.gov/geo/tiger/TIGER2020/BG/
    - Tracts: https://www2.census.gov/geo/tiger/TIGER2020/TRACT/
    """

    def __init__(self, timeout: int = 300, retries: int = 3):
        """
        Initialize downloader.

        Args:
            timeout: Request timeout in seconds
            retries: Number of retry attempts for failed downloads
        """
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"}
        )

    def download_blocks(
        self,
        state_fips: str,
        dest_dir: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """
        Download block shapefile for a state.

        Args:
            state_fips: 2-digit state FIPS code
            dest_dir: Destination directory
            progress_callback: Optional callback(downloaded, total) for progress

        Returns:
            Path to downloaded and extracted shapefile directory
        """
        url = TIGER_URLS["blocks"].format(state_fips=state_fips)
        return self._download_and_extract(url, dest_dir, progress_callback)

    def download_block_groups(
        self,
        state_fips: str,
        dest_dir: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """Download block group shapefile for a state."""
        url = TIGER_URLS["block_groups"].format(state_fips=state_fips)
        return self._download_and_extract(url, dest_dir, progress_callback)

    def download_tracts(
        self,
        state_fips: str,
        dest_dir: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """Download tract shapefile for a state."""
        url = TIGER_URLS["tracts"].format(state_fips=state_fips)
        return self._download_and_extract(url, dest_dir, progress_callback)

    def download_address_features(
        self,
        county_fips: str,
        dest_dir: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """
        Download address feature file for a county.

        Args:
            county_fips: 5-digit county FIPS code (state + county)
            dest_dir: Destination directory
            progress_callback: Optional callback for progress

        Returns:
            Path to downloaded and extracted shapefile directory
        """
        url = TIGER_URLS["addrfeat"].format(county_fips=county_fips)
        return self._download_and_extract(url, dest_dir, progress_callback)

    def download_address_features_for_state(
        self,
        state_fips: str,
        county_fips_list: List[str],
        dest_dir: Path,
        show_progress: bool = True,
    ) -> List[Path]:
        """
        Download address feature files for all counties in a state.

        Args:
            state_fips: 2-digit state FIPS code
            county_fips_list: List of 5-digit county FIPS codes
            dest_dir: Destination directory
            show_progress: Show progress bar

        Returns:
            List of paths to downloaded files
        """
        dest_dir.mkdir(parents=True, exist_ok=True)
        paths = []

        iterator = county_fips_list
        if show_progress:
            iterator = tqdm(county_fips_list, desc=f"Downloading address features for {state_fips}")

        for county_fips in iterator:
            try:
                path = self.download_address_features(county_fips, dest_dir)
                paths.append(path)
            except DownloadError as e:
                # Some counties may not have address feature files
                if e.status_code == 404:
                    continue
                raise

        return paths

    def _download_and_extract(
        self,
        url: str,
        dest_dir: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        """
        Download a ZIP file and extract it.

        Args:
            url: URL to download
            dest_dir: Destination directory
            progress_callback: Optional callback for progress

        Returns:
            Path to extracted directory
        """
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Determine output filename from URL
        filename = Path(urlparse(url).path).name
        zip_path = dest_dir / filename
        extract_dir = dest_dir / filename.replace(".zip", "")

        # Skip if already extracted
        if extract_dir.exists() and any(extract_dir.glob("*.shp")):
            return extract_dir

        # Download with retries
        for attempt in range(self.retries):
            try:
                self._download_file(url, zip_path, progress_callback)
                break
            except requests.RequestException as e:
                if attempt == self.retries - 1:
                    raise DownloadError(url, getattr(e.response, "status_code", 0), str(e))

        # Extract
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        # Clean up zip file
        zip_path.unlink()

        return extract_dir

    def _download_file(
        self,
        url: str,
        dest_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        chunk_size: int = 8192,
    ) -> Path:
        """
        Download a single file with progress tracking.

        Args:
            url: URL to download
            dest_path: Destination path
            progress_callback: Optional callback(downloaded, total)
            chunk_size: Download chunk size

        Returns:
            Path to downloaded file
        """
        response = self.session.get(url, stream=True, timeout=self.timeout)

        if response.status_code == 404:
            raise DownloadError(url, 404, "File not found")
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        progress_callback(downloaded, total_size)

        return dest_path


class CensusDataDownloader:
    """
    Downloads Census PL 94-171 data.

    Supports:
    1. Census API (limited to 50 variables per request)
    2. FTP bulk download (Legacy format)
    """

    API_BASE = "https://api.census.gov/data/2020/dec/pl"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 60):
        """
        Initialize Census data downloader.

        Args:
            api_key: Optional Census API key (increases rate limits)
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    def download_pl94171_for_state(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str = "block",
        dest_path: Optional[Path] = None,
    ) -> Path:
        """
        Download PL 94-171 data for a state via Census API.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to download
            geo_level: Geographic level (block, block group, tract, county)
            dest_path: Optional output path (CSV)

        Returns:
            Path to downloaded CSV file
        """
        # Build API request
        # Note: Census API has 50 variable limit per request
        # May need to batch for many variables

        geo_params = self._build_geo_params(state_fips, geo_level)
        all_data = []

        # Batch variables into groups of 50
        var_batches = [variables[i : i + 50] for i in range(0, len(variables), 50)]

        for batch in var_batches:
            params = {
                "get": ",".join(["GEO_ID"] + batch),
                "for": geo_params["for"],
                "in": geo_params.get("in", ""),
            }
            if self.api_key:
                params["key"] = self.api_key

            response = self.session.get(self.API_BASE, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            all_data.append(data)

        # Merge batches and save
        # First row is header
        import pandas as pd

        dfs = []
        for data in all_data:
            df = pd.DataFrame(data[1:], columns=data[0])
            dfs.append(df)

        if len(dfs) > 1:
            # Merge on GEO_ID
            result = dfs[0]
            for df in dfs[1:]:
                result = result.merge(df, on="GEO_ID", how="outer")
        else:
            result = dfs[0]

        # Save
        if dest_path is None:
            dest_path = Path(f"pl94171_{state_fips}_{geo_level}.csv")

        result.to_csv(dest_path, index=False)
        return dest_path

    def _build_geo_params(self, state_fips: str, geo_level: str) -> dict:
        """Build geographic parameters for Census API."""
        params = {}

        if geo_level == "block":
            params["for"] = "block:*"
            params["in"] = f"state:{state_fips} county:* tract:*"
        elif geo_level == "block group":
            params["for"] = "block group:*"
            params["in"] = f"state:{state_fips} county:* tract:*"
        elif geo_level == "tract":
            params["for"] = "tract:*"
            params["in"] = f"state:{state_fips} county:*"
        elif geo_level == "county":
            params["for"] = "county:*"
            params["in"] = f"state:{state_fips}"
        else:
            raise ValueError(f"Unknown geo_level: {geo_level}")

        return params


class ACSDataDownloader:
    """
    Downloads American Community Survey (ACS) 5-Year Estimates data.

    ACS provides richer demographic data than PL 94-171, including:
    - Income and poverty
    - Educational attainment
    - Employment and occupation
    - Housing characteristics
    - Health insurance
    - Commute patterns

    Note: ACS data is available at tract level and above, not at block level.
    For block-level data, use PL 94-171.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 120,
        year: int = 2020,
    ):
        """
        Initialize ACS data downloader.

        Args:
            api_key: Optional Census API key (increases rate limits)
            timeout: Request timeout in seconds
            year: ACS year (default 2020)
        """
        self.api_key = api_key
        self.timeout = timeout
        self.year = year
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"}
        )

    @property
    def api_base(self) -> str:
        """Get the API base URL for the configured year."""
        return f"https://api.census.gov/data/{self.year}/acs/acs5"

    def download_acs_for_state(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str = "tract",
        dest_path: Optional[Path] = None,
        show_progress: bool = True,
    ) -> Path:
        """
        Download ACS 5-Year data for a state via Census API.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to download
            geo_level: Geographic level (tract, block group, county)
                       Note: ACS is not available at block level
            dest_path: Optional output path (CSV)
            show_progress: Show download progress

        Returns:
            Path to downloaded CSV file
        """
        import pandas as pd

        # Validate geo_level
        valid_levels = ["tract", "block group", "county"]
        if geo_level not in valid_levels:
            raise ValueError(
                f"Invalid geo_level for ACS: {geo_level}. "
                f"ACS is available at: {', '.join(valid_levels)}"
            )

        geo_params = self._build_geo_params(state_fips, geo_level)
        all_data = []

        # Batch variables into groups of 50 (Census API limit)
        var_batches = [variables[i : i + 50] for i in range(0, len(variables), 50)]

        iterator = var_batches
        if show_progress:
            iterator = tqdm(var_batches, desc=f"Downloading ACS data for {state_fips}")

        for batch in iterator:
            params = {
                "get": ",".join(["GEO_ID", "NAME"] + batch),
                "for": geo_params["for"],
                "in": geo_params.get("in", ""),
            }
            if self.api_key:
                params["key"] = self.api_key

            response = self.session.get(
                self.api_base, params=params, timeout=self.timeout
            )

            if response.status_code == 400:
                # Check for invalid variable error
                error_msg = response.text
                raise DownloadError(
                    self.api_base,
                    400,
                    f"Invalid API request. Check variable names. {error_msg}",
                )
            response.raise_for_status()

            data = response.json()
            all_data.append(data)

        # Merge batches
        dfs = []
        for data in all_data:
            df = pd.DataFrame(data[1:], columns=data[0])
            dfs.append(df)

        if len(dfs) > 1:
            # Merge on GEO_ID, keeping NAME from first batch
            result = dfs[0]
            for df in dfs[1:]:
                # Drop NAME from subsequent batches to avoid duplicates
                df_no_name = df.drop(columns=["NAME"], errors="ignore")
                result = result.merge(df_no_name, on="GEO_ID", how="outer")
        else:
            result = dfs[0]

        # Clean up GEOID format
        # Census API returns GEO_ID like "1400000US06037101100" for tracts
        # We need just the numeric part: "06037101100"
        result["GEOID"] = result["GEO_ID"].str.extract(r"(\d+)$")[0]

        # Convert numeric columns
        for col in variables:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        # Save
        if dest_path is None:
            dest_path = Path(f"acs5_{state_fips}_{geo_level}_{self.year}.csv")

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(dest_path, index=False)
        return dest_path

    def _build_geo_params(self, state_fips: str, geo_level: str) -> dict:
        """Build geographic parameters for Census API."""
        params = {}

        if geo_level == "block group":
            params["for"] = "block group:*"
            params["in"] = f"state:{state_fips} county:* tract:*"
        elif geo_level == "tract":
            params["for"] = "tract:*"
            params["in"] = f"state:{state_fips} county:*"
        elif geo_level == "county":
            params["for"] = "county:*"
            params["in"] = f"state:{state_fips}"
        else:
            raise ValueError(f"Unknown geo_level: {geo_level}")

        return params

    def get_available_variables(self) -> dict:
        """
        Fetch list of available ACS variables from the API.

        Returns:
            Dictionary mapping variable codes to descriptions
        """
        url = f"{self.api_base}/variables.json"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        variables = data.get("variables", {})

        # Filter to only estimate variables (ending in E)
        return {
            k: v.get("label", "")
            for k, v in variables.items()
            if k.endswith("E") and not k.startswith("GEOCOMP")
        }
