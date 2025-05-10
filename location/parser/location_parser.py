import asyncio
import os
import time
from typing import Optional

import pandas as pd
from httpx import AsyncClient, RequestError, Response

from settings import settings
from logger import logger


class UniversityExcelFile:
    def __init__(self):
        self._from_data = settings.FILE_PATH

    @property
    def data(self):
        pd.set_option("display.max_colwidth", None)
        df = pd.read_excel(self._from_data, sheet_name="Sheet1", engine="openpyxl")
        return df

    @property
    def universities_names(self):
        return self.data["University Name"].to_list()

    def get_name_and_location(self):
        return self.data.set_index("University Name")["Location"].fillna(None).to_dict()

    def get_name_and_address(self):
        return self.data.set_index("University Name")["Address"].fillna(None).to_dict()


class GEOCoordinateParserByName:
    def __init__(self, names: list[str]):
        self.names = names
        self.url = settings.URL
        self.semaphore = asyncio.Semaphore(50)

    async def _make_request(
            self,
            client: AsyncClient,
            headers: dict,
            request_json:
            dict
    ) -> Optional[Response]:
        async with self.semaphore:
            try:
                response = await client.post(self.url, headers=headers, json=request_json, timeout=10)
                response.raise_for_status()
                return response
            except RequestError as e:
                logger.error(f"Request to {self.source} failed with error: {e}")
                return None

    async def _process_name(self, client: AsyncClient, name: str) -> dict:
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": settings.GEOCODING_API_KEY,
            "X-Goog-FieldMask": "places.location"
        }
        search_text = f"{name}".strip().replace("\xa0", " ").replace('\u200b', '')
        request_json = {"textQuery": search_text}

        response = await self._make_request(client, headers, request_json)
        if not response:
            logger.error(f"Something wrong with response: {response}")
            return name, None
        try:
            data = response.json()
            if "places" in data and data["places"]:
                location = data["places"][0]["location"]
                return name, {
                    'lat': location["latitude"],
                    'lng': location["longitude"]
                }
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

        logger.info(f"Failed to get coordinates for {len(no_coord)} universities")
        return name_and_coordinates

    def save_to_excel_file(self, data, filename="universities_coordinates.xlsx"):
        df = pd.DataFrame([
            {"university_name": name, "latitude": coord["lat"], "longitude": coord["lng"]}
            for name, coord in data.items() if coord
        ])
        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, "..", "outputs")
        os.makedirs(output_dir, exist_ok=True)
        save_path = os.path.join(output_dir, filename)
        df.to_excel(save_path, index=False)
        logger.info(f"Excel-файл сохранён по пути: {save_path}")


#loc_parser.save_to_excel_file(coord)
async def main():
    excel_file =UniversityExcelFile()
    names = excel_file.universities_names
    loc_parser = GEOCoordinateParserByName(names[:10])
    coord = await loc_parser.get_geo_coordinates()
    print(coord)


if __name__ == "__main__":
    asyncio.run(main())
