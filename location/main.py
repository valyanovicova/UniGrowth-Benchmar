import asyncio
from parser.excel_file import UniversityExcelFile
from parser.location_parser import GEOCoordinateParserByName


async def main():
    excel_file = UniversityExcelFile()
    names = excel_file.universities_names
    loc_parser = GEOCoordinateParserByName(names)
    coord = await loc_parser.get_geo_coordinates()
    excel_file.save_to_excel_file(coord)


if __name__ == "__main__":
    asyncio.run(main())
