import pandas as pd
import requests
import time
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
        return self.data.set_index("University Name")["Location"].to_dict()

    def get_name_and_address(self):
        return self.data.set_index("University Name")["Address"].to_dict()


class GEOCoordinateParserByName:
    def __init__(self, names):
        self.names = names

    def get_geo_coordinates_by_name(self):
        name_and_coordinates = {}
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        for name in self.names:
            params = {
                "address": name,
                "key": settings.GEOCODING_API_KEY
            }
            response = requests.get(url, params=params)
            if response.status_code != 200:
                logger.error(f"HTTP error for {name}: {response.status_code}")
                name_and_coordinates[name] = None
                continue
            data = response.json()

            status = data.get("status")
            if status != "OK":
                logger.warning(f"Geocoding failed for {name}: {status}. Response: {data}")
                name_and_coordinates[name] = None
                continue

            try:
                result = data["results"][0]
                location = result["geometry"]["location"]
                latitude = location["lat"]
                longitude = location["lng"]
                name_and_coordinates[name] = {'lat': latitude, 'lng': longitude}
            except (KeyError, IndexError):
                logger.error(f"Invalid structure for: {name}. Response: {data}")
                name_and_coordinates[name] = None

            time.sleep(0.2)

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
