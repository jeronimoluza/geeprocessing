import ee
import geopandas as gpd
import geemap
from src import h3funcs as h3f
from src import main

try:
    ee.Initialize(project="ee-jeronimol")
except Exception as e:
    ee.Authenticate()
    ee.Initialize()
print("Earth Engine API initialized successfully with default credentials.")

# Time period for the analysis
# START_YEAR = 1981
START_YEAR = 2024
END_YEAR = 2025

# GEE Export Configuration
EXPORT_FOLDER = "GEE_WEATHER_EXPORTS"
EXPORT_SCALE = 9000  # 9km resolution, similar to ERA5-Land

NAME = "testing"
# ID_PROPERTY = "hex_id"
# roi = h3f.h3_to_gdf(["862186aafffffff", "862186af7ffffff", "862186aafffffff"])

ID_PROPERTY = "aiyl"
roi = gpd.read_file("./data/grouped.geojson").head(100)

roi = geemap.geopandas_to_ee(roi)
main.process_single_region_batch(
    NAME,
    roi,
    ID_PROPERTY,
    start_year=2024,
    end_year=2024,
    export_hourly=True,
    export_seasonal=True,
    apply_gap_filling=True,
    scale=9000,
    folder_name=EXPORT_FOLDER,
    groups_of_n_months=1,
)
