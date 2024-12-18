from cache import Cache
from datetime import timedelta
import httpx
import os
import asyncio
import time
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TMDBRateLimiter:
    def __init__(self, requests_per_second: int = 40):
        self.requests_per_second = requests_per_second
        self.min_time_between_requests = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self.semaphore = asyncio.Semaphore(requests_per_second)

    async def wait(self):
        async with self.semaphore:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.min_time_between_requests:
                await asyncio.sleep(self.min_time_between_requests - time_since_last_request)
            self.last_request_time = time.time()


TMDB_POSTER_URL = "https://image.tmdb.org/t/p/w500"
TMDB_BACK_URL = "https://image.tmdb.org/t/p/original"
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# Cache configuration
tmp_cache = Cache(maxsize=100000, ttl=timedelta(days=7).total_seconds())

# Rate limiter instance
rate_limiter = TMDBRateLimiter(40)  # Setting to 40 requests per second to be safe


async def get_tmdb_data(client: httpx.AsyncClient, id: str, source: str) -> Dict[str, Any]:
    """
    Get TMDB data with improved rate limiting and error handling
    """
    if not id:
        logger.warning(f"Received empty ID for TMDB lookup")
        return {}

    # Check cache first
    cached_item = tmp_cache.get(id)
    if cached_item is not None:
        return cached_item

    params = {"external_source": source, "language": "it-IT", "api_key": TMDB_API_KEY}

    headers = {"accept": "application/json"}

    url = f"https://api.themoviedb.org/3/find/{id}"

    max_retries = 5
    base_delay = 2  # Base delay in seconds

    for attempt in range(max_retries):
        try:
            # Wait for rate limiter before making request
            await rate_limiter.wait()

            response = await client.get(url, headers=headers, params=params)

            if response.status_code == 200:
                meta_dict = response.json()
                meta_dict["imdb_id"] = id
                tmp_cache.set(id, meta_dict)
                return meta_dict

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", base_delay * (attempt + 1)))
                logger.warning(
                    f"Rate limited by TMDB. Waiting {retry_after} seconds. Attempt {attempt + 1}/{max_retries}"
                )
                await asyncio.sleep(retry_after)
                continue

            elif response.status_code >= 500:
                logger.error(f"TMDB server error: {response.status_code}. Attempt {attempt + 1}/{max_retries}")
                await asyncio.sleep(base_delay * (attempt + 1))
                continue

            else:
                logger.error(f"Unexpected status code from TMDB: {response.status_code}")
                return {}

        except Exception as e:
            logger.error(f"Error fetching TMDB data: {str(e)}")
            if attempt == max_retries - 1:
                return {}
            await asyncio.sleep(base_delay * (attempt + 1))

    return {}


async def batch_get_tmdb_data(client: httpx.AsyncClient, ids: list[str], source: str) -> list[Dict[str, Any]]:
    """
    Process multiple TMDB requests in batches to better manage rate limits
    """
    batch_size = 20  # Process 20 requests at a time
    results = []

    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i : i + batch_size]
        tasks = [get_tmdb_data(client, id, source) for id in batch_ids]
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)

        # Add a small delay between batches
        if i + batch_size < len(ids):
            await asyncio.sleep(0.5)

    return results
