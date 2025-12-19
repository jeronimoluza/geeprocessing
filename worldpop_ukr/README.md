This objective of this project is to obtain Age and Sex Structured WorldPop data for administrative levels of Ukraine

Area of interest: Ukraine


## Technologies
- Geopandas
- Rioxarray
- Xarray

## Source Files

- `src/run.py` - Serves as entrypoint and orchestrator
- `src/download.py` - Contains the code for downloading WorldPop data for Ukraine
- `src/file_managing.py` - Contains the code for unzipping the worldpop data and later deleting the tif files after processing
- `src/rasters.py` - Contains data necessary to clip and summarize statistics by geometries
- `src/shapes.py` - Handles geometrical operations and administratives area preparations

## Workflow

### Preparation step (src/shapes.py)

1. Download the data for Ukraine (from https://hub.worldpop.org/geodata/listing?id=138)
2. Use the adm3 and adm4 shapefiles provided by Avralt-Od
3. Calculate holes within each adm3 that do not correspond to an adm4, and add those to the adm4 database as "outskirts"
4. Store outputs:
   - `data/aoi/adm3.geojson` (columns: ADM0_PCODE, ADM1_PCODE, ADM2_PCODE, ADM3_PCODE, geometry)
   - `data/aoi/adm4_w_outskirts.geojson` (columns: ADM0_PCODE, ADM1_PCODE, ADM2_PCODE, ADM3_PCODE, ADM4_PCODE, outskirt, geometry)
   - Note: ADM4_PCODE column will contain the adm4_pcode if it corresponds to an adm4, or {adm3_pcode}_outskirts if it corresponds to a hole, and outskirt column will be 1 if it corresponds to an outskirt, and 0 otherwise

### Main workflow (src/run.py)

For each raster file and geometry, the processing involves clipping the raster to the geometry boundary and summing the number of individuals in all grid squares within that area. For example, `ukr_f_00_2015_CN_100m_R2025A_v1.tif` represents constrained estimates of total number of females of age group 0 to 12 months per grid square in Ukraine for 2015 at 100m resolution. When this file is clipped to an admin area and its values are summed, the result is the total number of females of age group 0 to 12 months living in that admin area.

#### ADM3 workflow

For each adm3 in `data/aoi/adm3.geojson`:
  For each WorldPop zip file in `data/worldpop/` folders:
    1. Unzip the file
    2. For each extracted .tif file:
       - Process the file (clip/summarize by adm3 geometry)
       - Delete the .tif file after processing (for storage reasons)

#### ADM4 with outskirts workflow

For each adm4 (including outskirts) in `data/aoi/adm4_w_outskirts.geojson`:
  For each WorldPop zip file in `data/worldpop/` folders:
    1. Unzip the file
    2. For each extracted .tif file:
       - Process the file (clip/summarize by adm4 geometry)
       - Delete the .tif file after processing (for storage reasons)


## Data sources

WorldPop Age and Sex Structures - Ukraine 2015-2030 (100m resolution) R2025A v1: https://hub.worldpop.org/geodata/listing?id=138

> Estimates of total number of people per grid square broken down by gender and age groupings (including 0-1 and by 5-year up to 90+) from 2015 to 2030 for Ukraine, R2025A version v1. The dataset is available to download in Geotiff format at a resolution of 3 arc (approximately 100m at the equator). The projection is Geographic Coordinate System, WGS84. The units are estimated number of male, female or both in each age group per grid square.

File Descriptions:

_{iso}\_{gender}\_{age group}\_{year}\_{type}\_{resolution}\_{release}\_{version}.tif_


| Field | Description |
|-------|-------------|
| iso | Three-letter country code |
| gender | m = male, f = female, t = both genders |
| age group | 00 = age group 0 to 12 months<br>01 = age group 1 to 4 years<br>05 = age group 5 to 9 years<br>90 = age 90 years and over |
| year | Year that the population represents |
| type | CN = Constrained |
| resolution | Resolution of the data e.g. 100m = 3 arc (approximately 100m at the equator) |
| release | Release |
| version | Version |

Example filename: ukr_f_00_2015_CN_100m_R2025A_v1.tif – this dataset represents constrained estimates of total number of females of age group 0 to 12 months per grid square in Ukraine for 2015 at 100m resolution, version R2025A v1.