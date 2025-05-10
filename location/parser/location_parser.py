import pandas as pd
import requests
import time
import aiohttp
import asyncio
import os
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
    def __init__(self, names: list):
        self.names = names

    def get_geo_coordinates(self):
        name_and_coordinates = {}
        no_coord = []
        url =  "https://places.googleapis.com/v1/places:searchText"
        headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": settings.GEOCODING_API_KEY,
                "X-Goog-FieldMask": "places.location"
            }
        for university_name in self.names:
            searchText = f"{university_name}".strip().replace("\xa0", " ").replace('\u200b', '')
            request_json = {"textQuery": searchText}
            response = requests.post(url, headers=headers, json=request_json)
            if response.status_code != 200:
                logger.error(f"HTTP error for {university_name}: {response.status_code}")
                name_and_coordinates[university_name] = None
                continue
            data_response = response.json()
            try:
                logger.info(f"Trying get coordinates for {university_name}")
                if "places" in data_response and len(data_response["places"]) > 0:
                    location = data_response["places"][0]["location"]
                    name_and_coordinates[university_name] = {
                        'lat': location["latitude"],
                        'lng': location["longitude"]
                    }
                else:
                    logger.warning(f"No results for: {university_name}. Response: {data_response}")
                    name_and_coordinates[university_name] = None
                    no_coord.append(university_name)
            except (KeyError, IndexError):
                logger.error(f"Invalid structure for: {university_name}. Response: {data_response}")
                name_and_coordinates[university_name] = None
            time.sleep(0.5)
        logger.info(f"Coudn't get coordinates for {no_coord}. Total item = {len(no_coord)}")
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

excel_file =UniversityExcelFile()
names = excel_file.universities_names
loc_parser = GEOCoordinateParserByName(names[:10])
coord = loc_parser.get_geo_coordinates()
print(coord)
#loc_parser.save_to_excel_file(coord)
