import ee
import pandas as pd
import geopandas as gpd
import geemap
from src import main
from src import pointfuncs
from shapely.geometry import Point


try:
    ee.Initialize(project="ee-jeronimol")
except Exception as e:
    ee.Authenticate()
    ee.Initialize()


# GEE Export Configuration
EXPORT_FOLDER = "GEE_WEATHER_EXPORTS"
EXPORT_SCALE = 9000  # 9km resolution, similar to ERA5-Land
BUFFER_ROI = 50_000  # 50km buffer

# KGZ params
# Time period for the analysis
# START_YEAR = 2021
# END_YEAR = 2025
# NAME = "KGZ"
# ID_PROPERTY = "aiyl"
# FILENAME = "l2kgz_gps.csv"
# LON_COL, LAT_COL = ("gps_lon", "gps_lat")

# UZB params
# Time period for the analysis
# START_YEAR = 2018
# END_YEAR = 2025
# NAME = "UZB"
# ID_PROPERTY = "Mahalla"
# FILENAME = "l2uzb_gps.csv"
# LON_COL, LAT_COL = ("longitude", "latitude")
# We're dropping 3 data points because their location is wrong:
# All 3 fall outside UZB
DROP_IDS_UZB = ["25750562", "25739392", "25863418"]

# KAZ params
# KAZ is currently being done on the centroids of the OCHA Administrative Level 2 Units
# Due to the inability to get surveyed household locations
# Time period for the analysis
# START_YEAR = 2020
# END_YEAR = 2025
# NAME = "KAZ"
# ID_PROPERTY = "ADM2_PCODE"
# FILENAME = "l2kaz_gps_proxy.csv"
# LON_COL, LAT_COL = ("long", "lat")

# TJK params
# Time period for the analysis
START_YEAR = 2015
END_YEAR = 2025
NAME = "TJK"
ID_PROPERTY = "clusterid12"
FILENAME = "l2tjk_gps.csv"
LON_COL, LAT_COL = ("gps_long", "gps_lat")

# IDN Baseline 1 params
# Time period for the analysis
# START_YEAR = 2023
# END_YEAR = 2025
# NAME = "IDN_B1"
# ID_PROPERTY = "wilcah"
# FILENAME = "l2idn_gps_b1.csv"
# LON_COL, LAT_COL = ("longitude", "latitude")


# IDN Baseline 2 params
# Point HHID = 46804 appears to be outside of it's 50km buffer!
# Time period for the analysis
# START_YEAR = 2025
# END_YEAR = 2025
# NAME = "IDN_B2"
# ID_PROPERTY = "wilcah"
# FILENAME = "l2idn_gps_b2.csv"
# LON_COL, LAT_COL = ("longitude", "latitude")
# 14 HHID have their lat long columns missing, so we're dropping them.
DROP_IDS_IDN_B2 = [
    7702,
    39905,
    39906,
    28307,
    1302,
    1307,
    1308,
    39402,
    27308,
    27309,
    24610,
    24605,
    24608,
    38908,
]


points = pd.read_csv(f"./data/{FILENAME}")
if NAME == "UZB":
    points = points[~points["L2CU ID"].isin(DROP_IDS_UZB)]

if NAME == "IDN_B2":
    points = points[~points["hhid"].isin(DROP_IDS_IDN_B2)]

points["geometry"] = points.apply(
    lambda row: Point(row[LON_COL], row[LAT_COL]), axis=1
)
points = gpd.GeoDataFrame(points, crs=4326).to_crs(3857)

centroids = (
    pointfuncs.group_points(points, [ID_PROPERTY])
    .drop([LAT_COL, LON_COL], axis=1)
    .to_crs(4326)
)

roi = centroids.copy()
roi["geometry"] = roi.geometry.to_crs(3857).buffer(BUFFER_ROI).to_crs(4326)

# For kepler.gl
roi.to_wkt().to_csv(
    f"./data/{NAME}_roi_buffer_{int(BUFFER_ROI / 1000)}_km.csv", index=None
)
# roi.to_file(f"./data/shp/roi_buffer_{int(BUFFER_ROI / 1000)}_km.shp", driver='ESRI Shapefile')
roi = geemap.geopandas_to_ee(roi)

main.process_single_region_batch(
    NAME,
    roi,
    ID_PROPERTY,
    start_year=START_YEAR,
    end_year=END_YEAR,
    export_hourly=True,
    export_seasonal=False,
    apply_gap_filling=True,
    scale=9000,
    folder_name=EXPORT_FOLDER,
    groups_of_n_months=1,
)
