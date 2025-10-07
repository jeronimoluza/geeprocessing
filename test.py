import ee
import geemap
from src import h3funcs as h3f
from src import main

service_account = "testing-era5@ee-jeronimol.iam.gserviceaccount.com"
key_file_path = "./configs/auth/ee-jeronimol-02e92f05b863.json"

try:
    credentials = ee.ServiceAccountCredentials(service_account, key_file_path)
    ee.Initialize(credentials)
    print("Earth Engine API initialized successfully with service account.")
except Exception as e:
    print(f"Service account initialization failed: {e}")
    print("Falling back to default authentication.")
    try:
        ee.Initialize()
    except Exception as e:
        ee.Authenticate()
        ee.Initialize()
    print(
        "Earth Engine API initialized successfully with default credentials."
    )

    # A Sentinel-2 surface reflectance image.
img = ee.Image("COPERNICUS/S2_SR/20210109T185751_20210109T185931_T10SEG")
m = geemap.Map()
m.set_center(-122.359, 37.428, 9)
m.add_layer(
    img, {"bands": ["B11", "B8", "B3"], "min": 100, "max": 3500}, "img"
)

# Sample the image at 20 m scale, a point feature collection is returned.
samp = img.sample(scale=20, numPixels=50, geometries=True)
m.add_layer(samp, {"color": "white"}, "samp")

# Export the image sample feature collection to Drive as a CSV file.
task = ee.batch.Export.table.toDrive(
    collection=samp,
    description="image_sample_demo_csv",
    folder="earth_engine_demos",
    fileFormat="CSV",
)
task.start()

# Export a subset of collection properties: three bands and the geometry
# as GeoJSON.
task = ee.batch.Export.table.toDrive(
    collection=samp,
    description="image_sample_demo_prop_subset",
    folder="earth_engine_demos",
    fileFormat="GeoJSON",
    selectors=["B8", "B11", "B12", ".geo"],
)
task.start()

# Export the image sample feature collection to Drive as a shapefile.
task = ee.batch.Export.table.toDrive(
    collection=samp,
    description="image_sample_demo_shp",
    folder="earth_engine_demos",
    fileFormat="SHP",
)
task.start()
