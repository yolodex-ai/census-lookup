"""Download TIGER/Line and Census data files."""

import asyncio
import zipfile
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, List, Optional

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
        self._pending: Dict[str, asyncio.Task[Path | None]] = {}

    async def download_once(
        self,
        resource_key: str,
        download_func: Callable[[], Coroutine[Any, Any, Path | None]],
    ) -> Path | None:
        """
        Ensure a resource is downloaded only once, even with concurrent requests.

        If a download is already in progress for the resource, wait for it.
        Otherwise, start the download.

        Args:
            resource_key: Unique identifier for the resource (e.g., "blocks/06")
            download_func: Async function that performs the download

        Returns:
            Path to the downloaded resource, or None if not available
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
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
            )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session:
            await self._session.close()
            self._session = None

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

        async def do_download() -> Path:
            return await self._download_and_extract(url, dest_dir)

        result = await _coordinator.download_once(resource_key, do_download)
        assert result is not None  # do_download always returns Path
        return result

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

        async def do_download() -> Path:
            return await self._download_and_extract(url, dest_dir)

        result = await _coordinator.download_once(resource_key, do_download)
        assert result is not None  # do_download always returns Path
        return result

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

        async def download_county(county_fips: str) -> Path:
            async with semaphore:
                return await self.download_address_features(county_fips, dest_dir)

        # Create tasks for all counties
        tasks = [download_county(fips) for fips in county_fips_list]

        # Execute with progress bar
        paths = []
        desc = f"Downloading address features for {state_fips}"
        with tqdm(total=len(tasks), desc=desc) as pbar:
            for coro in asyncio.as_completed(tasks):
                result = await coro
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
        for attempt in range(self.retries):  # pragma: no branch
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
                    f.write(chunk)

        return dest_path


class CensusDataDownloader:
    """
    Downloads Census PL 94-171 data from bulk files on Census FTP.

    Downloads pre-built zip files which are much faster and more reliable
    than the Census API for large states.

    Data source: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/
    """

    def __init__(self, timeout: int = 600, retries: int = 3):
        """
        Initialize Census data downloader.

        Args:
            timeout: Request timeout in seconds (default 10 minutes for large files)
            retries: Number of retry attempts for failed downloads
        """
        self.timeout = timeout
        self.retries = retries
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Create aiohttp session for download."""
        # Session is created fresh for each download operation
        # Coordinator prevents concurrent downloads of same resource
        assert self._session is None, "Session already exists - possible re-entry bug"
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
        )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def download_pl94171_for_state(
        self,
        state_fips: str,
        variables: List[str],
        dest_path: Path,
        show_progress: bool = True,
    ) -> Path:
        """
        Download PL 94-171 data for a state from Census bulk files.

        Downloads the state's zip file, parses it, and saves as parquet.
        Uses coordination to prevent duplicate downloads.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to include
            dest_path: Output path for parquet file
            show_progress: Show download progress bar

        Returns:
            Path to downloaded parquet file
        """
        resource_key = f"pl94171/{state_fips}/block"

        async def do_download() -> Path:
            return await self._download_pl94171_bulk(
                state_fips, variables, dest_path, show_progress=show_progress
            )

        result = await _coordinator.download_once(resource_key, do_download)
        assert result is not None
        return result

    async def _download_pl94171_bulk(
        self,
        state_fips: str,
        variables: List[str],
        dest_path: Path,
        show_progress: bool = True,
    ) -> Path:
        """Download PL 94-171 bulk zip file and parse it."""
        from census_lookup.data.pl94171_parser import get_pl94171_url, parse_pl94171_zip

        url = get_pl94171_url(state_fips)
        session = await self._get_session()

        # Download zip file to temp location
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        zip_path = dest_path.parent / f"pl94171_{state_fips}.zip"

        for attempt in range(self.retries):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()

                    total_size = response.content_length or 0

                    pbar = None
                    if show_progress:
                        pbar = tqdm(
                            total=total_size,
                            desc="  Downloading PL 94-171",
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                        )

                    with open(zip_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                            if pbar:
                                pbar.update(len(chunk))

                    if pbar:
                        pbar.close()

                    break  # Success

            except (
                aiohttp.ClientPayloadError,
                aiohttp.ClientConnectionError,
                asyncio.TimeoutError,
            ) as e:
                if zip_path.exists():
                    zip_path.unlink()
                if attempt < self.retries - 1:
                    wait_time = 2**attempt
                    if show_progress:
                        print(f"  Retry {attempt + 1}/{self.retries} in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    raise e

        # Parse zip file
        if show_progress:
            print("  Parsing PL 94-171 data...")

        df = parse_pl94171_zip(zip_path, variables=variables, summary_level="750")

        # Rename GEOID to GEO_ID for compatibility with existing code
        df = df.rename(columns={"GEOID": "GEO_ID"})

        # Save as parquet
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest_path, index=False)

        # Clean up zip file
        zip_path.unlink()

        return dest_path


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
        """Create aiohttp session for download."""
        # Session is created fresh for each download operation
        # Coordinator prevents concurrent downloads of same resource
        assert self._session is None, "Session already exists - possible re-entry bug"
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": "census-lookup/0.1.0 (https://github.com/census-lookup)"},
        )
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def api_base(self) -> str:
        """Get the API base URL for the configured year."""
        return f"https://api.census.gov/data/{self.year}/acs/acs5"

    async def download_acs_for_state(
        self,
        state_fips: str,
        variables: List[str],
        dest_path: Path,
    ) -> Path:
        """
        Download ACS 5-Year data for a state via Census API (tract level).

        Uses coordination to prevent duplicate downloads.
        Variable batches are downloaded concurrently.

        Args:
            state_fips: 2-digit state FIPS code
            variables: List of variable codes to download
            dest_path: Output path for CSV file

        Returns:
            Path to downloaded CSV file
        """
        resource_key = f"acs/{state_fips}/tract/{self.year}"

        async def do_download() -> Path:
            return await self._download_acs(state_fips, variables, dest_path)

        result = await _coordinator.download_once(resource_key, do_download)
        assert result is not None  # do_download always returns Path
        return result

    async def _download_acs(
        self,
        state_fips: str,
        variables: List[str],
        dest_path: Path,
    ) -> Path:
        """Internal implementation for ACS download (tract level only)."""
        import pandas as pd

        session = await self._get_session()
        geo_params = self._build_geo_params(state_fips)

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
                # Drop columns that would duplicate during merge
                cols_to_drop = ["NAME", "state", "county", "tract"]
                df_clean = df.drop(columns=cols_to_drop, errors="ignore")
                result = result.merge(df_clean, on="GEO_ID", how="outer")
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

    def _build_geo_params(self, state_fips: str) -> dict[str, str]:
        """Build geographic parameters for Census API (tract level only)."""
        return {
            "for": "tract:*",
            "in": f"state:{state_fips} county:*",
        }
