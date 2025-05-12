import pandas as pd
from settings import settings


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
