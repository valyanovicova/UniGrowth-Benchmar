import asyncio
from typing import Optional
from collections import deque

from tenacity import retry, stop_after_attempt, wait_exponential
from httpx import (
    AsyncClient,
    RequestError,
    Response,
    NetworkError,
    TimeoutException,
    HTTPStatusError
)

from settings import settings
from logger import logger
from parser.cache import AsyncCacheFile
from parser.excel_file import UniversityExcelFile


class QPSLimiter:
    def __init__(self, qps):
        self.qps = qps
        self.timestamps = deque(maxlen=qps)

    async def wait(self):
        now = asyncio.get_event_loop().time()
        if len(self.timestamps) >= self.qps:
            elapsed = now - self.timestamps[0]
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
        self.timestamps.append(now)


class GEOCoordinateParserByName:
    def __init__(self, names: list[str]):
        self.names = names
        self.url = settings.URL
        self.semaphore = asyncio.Semaphore(settings.QPS)
        self.timeout = settings.TIMEOUT
        self.qps_limiter = QPSLimiter(50)
        self.cache = AsyncCacheFile()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
    async def _make_request(
            self,
            client: AsyncClient,
            headers: dict,
            request_json:
            dict
    ) -> Optional[Response]:
        async with self.semaphore:

            logger.debug(
                f"Sending request to {self.url}\n"
                f"Headers: {headers}\n"
                f"json: {request_json}"
            )
            await self.qps_limiter.wait()
            response = await client.post(
                self.url,
                headers=headers,
                json=request_json,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response

    async def _process_name(self, client: AsyncClient, name: str) -> dict:

        if await self.cache.contains(name):
            return name, await self.cache.get(name)

        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": settings.GEOCODING_API_KEY,
            "X-Goog-FieldMask": "places.location"
        }
        search_text = f"{name}".strip().replace("\xa0", " ").replace('\u200b', '')
        request_json = {"textQuery": search_text}
        try:
            response = await self._make_request(client, headers, request_json)
        except HTTPStatusError as e:
            logger.error(f"Error: {e} for {request_json}")
            raise
        except RequestError as e:
            logger.error(
                f"Request to {self.url} with search params: {request_json} failed with error: {e}"
            )
            return None
        except TimeoutException:
            logger.error(f"Timeout: Not response in {self.timeout}")
            return None
        except NetworkError:
            logger.error("Check network connection")
            raise
        if response is None:
            logger.error(f"Response is None for {search_text}")
            return None
        try:
            data = response.json()
            if "places" in data and data["places"]:
                location = data["places"][0]["location"]
                result = {
                    'lat': location["latitude"],
                    'lng': location["longitude"]
                }
                await self.cache.set(name, result)
                return name, result
            logger.warning(f"No results for: {name}. Response: {data}")
            return name, None
        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"Invalid response for {name}: {str(e)}")
            return name, None

    async def get_geo_coordinates(self) -> dict[str, dict[str, float]]:
        async with AsyncClient() as client:
            tasks = [self._process_name(client, name) for name in self.names]
            results = await asyncio.gather(*tasks)

        name_and_coordinates = {}
        no_coord = []

        for name, result in results:
            name_and_coordinates[name] = result
            if result is None:
                no_coord.append(name)

        logger.info(f"Failed to get coordinates for {len(no_coord)} universities\n\
                    {no_coord}")
        return name_and_coordinates


async def main():
    excel_file = UniversityExcelFile()
    names = excel_file.universities_names
    loc_parser = GEOCoordinateParserByName(names)
    coord = await loc_parser.get_geo_coordinates()
    excel_file.save_to_excel_file(coord)


if __name__ == "__main__":
    asyncio.run(main())
