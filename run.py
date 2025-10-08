import ee
import pandas as pd
import geopandas as gpd
import geemap
from src import h3funcs as h3f
from src import main
from src import pointfuncs
from shapely.geometry import Point


try:
    ee.Initialize(project="ee-jeronimol")
except Exception as e:
    ee.Authenticate()
    ee.Initialize()
print("Earth Engine API initialized successfully with default credentials.")

# Time period for the analysis
# START_YEAR = 1981
START_YEAR = 2024
END_YEAR = 2024

# GEE Export Configuration
EXPORT_FOLDER = "GEE_WEATHER_EXPORTS"
EXPORT_SCALE = 9000  # 9km resolution, similar to ERA5-Land
BUFFER_ROI = 50_000  # 50km buffer

NAME = "KGZ"
ID_PROPERTY = "aiyl"

points = pd.read_csv("./data/l2kgz_gps.csv")
points["geometry"] = points.apply(
    lambda row: Point(row["gps_lon"], row["gps_lat"]), axis=1
)
points = gpd.GeoDataFrame(points, crs=4326).to_crs(3857)

centroids = (
    pointfuncs.group_points(points, ["aiyl"])
    .drop(["gps_lat", "gps_lon"], axis=1)
    .to_crs(4326)
)

roi = centroids.copy()
roi["geometry"] = roi.geometry.to_crs(3857).buffer(BUFFER_ROI).to_crs(4326)

roi.to_wkt().to_csv(
    f"./data/roi_buffer_{int(BUFFER_ROI / 1000)}_km.csv", index=None
)
roi.to_file(f"./data/roi_buffer_{int(BUFFER_ROI / 1000)}_km.shp", driver='ESRI Shapefile')
raise

roi = geemap.geopandas_to_ee(roi)
main.process_single_region_batch(
    NAME,
    roi,
    ID_PROPERTY,
    start_year=2024,
    end_year=2024,
    export_hourly=True,
    export_seasonal=False,
    apply_gap_filling=True,
    scale=9000,
    folder_name=EXPORT_FOLDER,
    groups_of_n_months=1,
)
