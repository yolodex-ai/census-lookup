"""Download TIGER/Line and Census data files."""

import asyncio
import zipfile
from pathlib import Path
from typing import Callable, Dict, List, Optional

import aiohttp
from tqdm import tqdm

from census_lookup.data.constants import TIGER_URLS


class DownloadError(Exception):
    """Error downloading a file."""

    def __init__(self, url: str, status_code: int, message: str = ""):
        self.url = url
        self.status_code = status_code
        super().__init__(f"Failed to download {url}: HTTP {status_code}. {message}")


class DownloadCoordinator:
    """
    Coordinates concurrent downloads to prevent duplicate requests.

    Uses asyncio Events to allow multiple waiters to share a single download.
    When a download starts, other requesters wait for it to complete rather
    than starting duplicate downloads.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._pending: Dict[str, asyncio.Task] = {}

    async def download_once(
        self,
        resource_key: str,
        download_func: Callable[[], "asyncio.Future[Path]"],
    ) -> Path:
        """
        Ensure a resource is downloaded only once, even with concurrent requests.

        If a download is already in progress for the resource, wait for it.
        Otherwise, start the download.

        Args:
            resource_key: Unique identifier for the resource (e.g., "blocks/06")
            download_func: Async function that performs the download

        Returns:
            Path to the downloaded resource
        """
        async with self._lock:
            # Check if there's already a pending download for this resource
            if resource_key in self._pending:
                task = self._pending[resource_key]
            else:
                # Start a new download task
                task = asyncio.create_task(download_func())
                self._pending[resource_key] = task

        try:
            # Wait for the task (either we created it or we're waiting on existing one)
            result = await task
            return result
        finally:
            # Clean up only if we're the one who started it and it's done
            async with self._lock:
                if resource_key in self._pending and self._pending[resource_key].done():
                    del self._pending[resource_key]


# Global coordinator instance
_coordinator = DownloadCoordinator()


def _extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract a zip file and clean up."""
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    zip_path.unlink()


class TIGERDownloader:
    """
    Downloads TIGER/Line shapefiles from Census Bureau.

    Data sources:
    - Address Features: https://www2.census.gov/geo/tiger/TIGER2020/ADDRFEAT/
    - Blocks: https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/
    - Block Groups: https://www2.census.gov/geo/tiger/TIGER2020/BG/
    - Tracts: https://www2.census.gov/geo/tiger/TIGER2020/TRACT/

    All methods are async and use aiohttp for concurrent downloads.
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
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def download_blocks(
        self,
        state_fips: str,
        dest_dir: Path,
    ) -> Path:
        """
        Download block shapefile for a state.

        Uses coordination to prevent duplicate downloads.

        Args:
            state_fips: 2-digit state FIPS code
            dest_dir: Destination directory

        Returns:
            Path to downloaded and extracted shapefile directory
        """
        url = TIGER_URLS["blocks"].format(state_fips=state_fips)
        resource_key = f"blocks/{state_fips}"

        async def do_download():
            return await self._download_and_extract(url, dest_dir)

        return await _coordinator.download_once(resource_key, do_download)

    async def download_address_features(
        self,
        county_fips: str,
        dest_dir: Path,
    ) -> Path:
        """
        Download address feature file for a county.

        Uses coordination to prevent duplicate downloads.

        Args:
            county_fips: 5-digit county FIPS code (state + county)
            dest_dir: Destination directory

        Returns:
            Path to downloaded and extracted shapefile directory
        """
        url = TIGER_URLS["addrfeat"].format(county_fips=county_fips)
        resource_key = f"addrfeat/{county_fips}"

        async def do_download():
            return await self._download_and_extract(url, dest_dir)

        return await _coordinator.download_once(resource_key, do_download)

    async def download_address_features_for_state(
        self,
        state_fips: str,
        county_fips_list: List[str],
        dest_dir: Path,
        max_concurrent: int = 10,
    ) -> List[Path]:
        """
        Download address feature files for all counties in a state.

        Downloads multiple counties concurrently for better performance.
        Uses coordination to prevent duplicate downloads.

        Args:
            state_fips: 2-digit state FIPS code
            county_fips_list: List of 5-digit county FIPS codes
            dest_dir: Destination directory
            max_concurrent: Maximum concurrent downloads

        Returns:
            List of paths to downloaded files
        """
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Use semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_concurrent)

        async def download_county(county_fips: str) -> Optional[Path]:
            async with semaphore:
                try:
                    return await self.download_address_features(county_fips, dest_dir)
                except DownloadError as e:
                    if e.status_code == 404:
                        return None
                    raise

        # Create tasks for all counties
        tasks = [download_county(fips) for fips in county_fips_list]

        # Execute with progress bar
        paths = []
        desc = f"Downloading address features for {state_fips}"
        with tqdm(total=len(tasks), desc=desc) as pbar:
            for coro in asyncio.as_completed(tasks):
                result = await coro
                if result is not None:
                    paths.append(result)
                pbar.update(1)

        return paths

    async def _download_and_extract(
        self,
        url: str,
        dest_dir: Path,
    ) -> Path:
        """
        Download a ZIP file and extract it.

        Args:
            url: URL to download
            dest_dir: Destination directory

        Returns:
            Path to extracted directory
        """
        from urllib.parse import urlparse

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
                await self._download_file(url, zip_path)
                break
            except aiohttp.ClientError as e:
                if attempt == self.retries - 1:
                    raise DownloadError(url, 0, str(e))

        # Extract (run in executor to not block event loop)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _extract_zip, zip_path, extract_dir)

        return extract_dir

    async def _download_file(
        self,
        url: str,
        dest_path: Path,
        chunk_size: int = 8192,
    ) -> Path:
        """
        Download a single file.

        Args:
            url: URL to download
            dest_path: Destination path
            chunk_size: Download chunk size

        Returns:
            Path to downloaded file
        """
        session = await self._get_session()

        async with session.get(url) as response:
            if response.status == 404:
                raise DownloadError(url, 404, "File not found")
            response.raise_for_status()

            with open(dest_path, "wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    if chunk:
                        f.write(chunk)

        return dest_path


class CensusDataDownloader:
    """
    Downloads Census PL 94-171 data via the Census API.

    All methods are async and use aiohttp for concurrent downloads.
    Variable batches are downloaded concurrently when there are more
    than 50 variables (Census API limit per request).
    """

    API_BASE = "https://api.census.gov/data/2020/dec/pl"

    def __init__(self, timeout: int = 60):
        """
        Initialize Census data downloader.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def download_pl94171_for_state(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str,
        dest_path: Path,
    ) -> Path:
        """
        Download PL 94-171 data for a state via Census API.

        Uses coordination to prevent duplicate downloads.
        Variable batches are downloaded concurrently.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to download
            geo_level: Geographic level (block, block group, tract, county)
            dest_path: Output path for CSV file

        Returns:
            Path to downloaded CSV file
        """
        resource_key = f"pl94171/{state_fips}/{geo_level}"

        async def do_download():
            return await self._download_pl94171(state_fips, variables, geo_level, dest_path)

        return await _coordinator.download_once(resource_key, do_download)

    async def _download_pl94171(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str,
        dest_path: Path,
    ) -> Path:
        """Internal implementation for PL 94-171 download."""
        import pandas as pd

        session = await self._get_session()
        geo_params = self._build_geo_params(state_fips, geo_level)

        # Batch variables into groups of 50
        var_batches = [variables[i : i + 50] for i in range(0, len(variables), 50)]

        # Download batches concurrently
        async def fetch_batch(batch: List[str]):
            params = {
                "get": ",".join(["GEO_ID"] + batch),
                "for": geo_params["for"],
                "in": geo_params.get("in", ""),
            }

            url = f"{self.API_BASE}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

        tasks = [fetch_batch(batch) for batch in var_batches]
        all_data = await asyncio.gather(*tasks)

        # Merge batches
        dfs = []
        for data in all_data:
            df = pd.DataFrame(data[1:], columns=data[0])
            dfs.append(df)

        if len(dfs) > 1:
            result = dfs[0]
            for df in dfs[1:]:
                result = result.merge(df, on="GEO_ID", how="outer")
        else:
            result = dfs[0]

        result.to_csv(dest_path, index=False)
        return dest_path

    def _build_geo_params(self, state_fips: str, geo_level: str) -> dict:
        """Build geographic parameters for Census API.

        Note: PL 94-171 data is always downloaded at block level.
        Higher geographic levels are derived by aggregating block data.
        """
        if geo_level != "block":
            raise ValueError(
                f"PL 94-171 download only supports block level, got: {geo_level}. "
                "Higher levels are derived from block data."
            )

        return {
            "for": "block:*",
            "in": f"state:{state_fips} county:* tract:*",
        }


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

    All methods are async and use aiohttp for concurrent downloads.
    """

    def __init__(
        self,
        timeout: int = 120,
        year: int = 2020,
    ):
        """
        Initialize ACS data downloader.

        Args:
            timeout: Request timeout in seconds
            year: ACS year (default 2020)
        """
        self.timeout = timeout
        self.year = year
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def api_base(self) -> str:
        """Get the API base URL for the configured year."""
        return f"https://api.census.gov/data/{self.year}/acs/acs5"

    async def download_acs_for_state(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str,
        dest_path: Path,
    ) -> Path:
        """
        Download ACS 5-Year data for a state via Census API.

        Uses coordination to prevent duplicate downloads.
        Variable batches are downloaded concurrently.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to download
            geo_level: Geographic level (tract, block group, county)
                       Note: ACS is not available at block level
            dest_path: Output path for CSV file

        Returns:
            Path to downloaded CSV file
        """
        resource_key = f"acs/{state_fips}/{geo_level}/{self.year}"

        async def do_download():
            return await self._download_acs(state_fips, variables, geo_level, dest_path)

        return await _coordinator.download_once(resource_key, do_download)

    async def _download_acs(
        self,
        state_fips: str,
        variables: List[str],
        geo_level: str,
        dest_path: Path,
    ) -> Path:
        """Internal implementation for ACS download."""
        import pandas as pd

        # Validate geo_level
        valid_levels = ["tract", "block group", "county"]
        if geo_level not in valid_levels:
            raise ValueError(
                f"Invalid geo_level for ACS: {geo_level}. "
                f"ACS is available at: {', '.join(valid_levels)}"
            )

        session = await self._get_session()
        geo_params = self._build_geo_params(state_fips, geo_level)

        # Batch variables into groups of 50 (Census API limit)
        var_batches = [variables[i : i + 50] for i in range(0, len(variables), 50)]

        async def fetch_batch(batch: List[str]):
            params = {
                "get": ",".join(["GEO_ID", "NAME"] + batch),
                "for": geo_params["for"],
                "in": geo_params.get("in", ""),
            }

            url = f"{self.api_base}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
            async with session.get(url) as response:
                if response.status == 400:
                    error_msg = await response.text()
                    raise DownloadError(
                        self.api_base,
                        400,
                        f"Invalid API request. Check variable names. {error_msg}",
                    )
                response.raise_for_status()
                return await response.json()

        # Fetch all batches concurrently with progress
        all_data = []
        desc = f"Downloading ACS data for {state_fips}"
        with tqdm(total=len(var_batches), desc=desc) as pbar:
            for coro in asyncio.as_completed([fetch_batch(b) for b in var_batches]):
                result = await coro
                all_data.append(result)
                pbar.update(1)

        # Merge batches
        dfs = []
        for data in all_data:
            df = pd.DataFrame(data[1:], columns=data[0])
            dfs.append(df)

        if len(dfs) > 1:
            result = dfs[0]
            for df in dfs[1:]:
                df_no_name = df.drop(columns=["NAME"], errors="ignore")
                result = result.merge(df_no_name, on="GEO_ID", how="outer")
        else:
            result = dfs[0]

        # Clean up GEOID format
        result["GEOID"] = result["GEO_ID"].str.extract(r"(\d+)$")[0]

        # Convert numeric columns
        for col in variables:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

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