import os

import pandas as pd

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
        logger.info(f"Excel-file was saved: {save_path}")
