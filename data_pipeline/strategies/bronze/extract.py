"""Bronze extraction strategy for OpenBreweryDB API pagination."""

import asyncio
import math

import aiohttp
import backoff
import requests

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class BronzeAsyncAPIExtract(Strategy):
    """Extract brewery records asynchronously from OpenBreweryDB."""

    default_per_page = 200

    def __init__(self, concurrency: int = 5):
        """Create extractor with bounded concurrency.

        Args:
            concurrency: Maximum number of in-flight HTTP requests.
        """
        self.semaphore = asyncio.Semaphore(concurrency)
        self.logger = StructuredLogger("strategy-bronze-extract")

    @backoff.on_exception(
        backoff.expo,
        aiohttp.ClientError,
        max_tries=5,
        jitter=backoff.full_jitter,
    )
    async def _fetch(self, session, url, params):
        """Fetch one page from the API with retry support.

        Args:
            session: Active aiohttp session.
            url: API endpoint URL.
            params: Query string parameters.

        Returns:
            JSON payload for a single page.
        """

        async with self.semaphore:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                return await response.json()

    async def _gather_pages(self, url, base_params, total_pages):
        """Request all pages concurrently and flatten results.

        Args:
            url: API endpoint URL.
            base_params: Base query parameters.
            total_pages: Number of pages to request.

        Returns:
            Flattened list of brewery records.
        """

        async with aiohttp.ClientSession() as session:
            tasks = []

            for page in range(1, total_pages + 1):
                params = {**base_params, "page": page}
                tasks.append(self._fetch(session, url, params))

            responses = await asyncio.gather(*tasks)
            return [item for sublist in responses for item in sublist]

    def get_total_pages(self, url):
        """Compute total pages from the metadata endpoint.

        Args:
            url: Metadata endpoint URL.

        Returns:
            Total number of pages based on configured page size.
        """
        response = requests.get(url)
        data = response.json()
        total_entries = data["total"]
        self.logger.info(
            "Bronze metadata fetched",
            total_entries=total_entries,
            per_page=self.default_per_page,
        )

        return math.ceil(total_entries / self.default_per_page)

    def execute(self, context):
        """Run extraction for all API pages.

        Args:
            context: Runtime settings including metadata and list endpoints.

        Returns:
            List of extracted brewery records.
        """

        params = context.get("params", {"per_page": self.default_per_page})

        total_pages = self.get_total_pages(context["metadata_breweries_url"])
        self.logger.info("Bronze extraction started", total_pages=total_pages)

        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(
            self._gather_pages(
                context["list_breweries_url"], params, total_pages
            )
        )
        self.logger.info("Bronze extraction finished", records=len(data))

        return data
