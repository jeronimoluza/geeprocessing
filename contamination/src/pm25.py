import rioxarray as rxr 
import geopandas as gpd
from loading import load_hhs_data
import os
from tqdm import tqdm
import pandas as pd

PM25_DATA = "../data/pm25/stlouis/2023/"
HHS_DATA = "../data/hhs/l2idn_gps_b2.csv"
OUTPUT_FILE = "../outputs/pm25/hhs_pm25.csv"

if __name__ == "__main__":
    hhs_gdf = load_hhs_data(HHS_DATA)

    files = os.listdir(PM25_DATA)
    output = []
    for file in tqdm(files):
        date = file.split("-")[1].split(".")[0]
        if file.endswith(".nc"):
            ds = rxr.open_rasterio(PM25_DATA + file)
            for _, row in hhs_gdf.iterrows():
                pm25 = ds.sel(x=row.geometry.x, y=row.geometry.y, method="nearest").values.squeeze()
                row["pm25"] = pm25
                row['date'] = pd.to_datetime(date, format="%Y%m").strftime("%Y-%m-%d")
                output.append(row)
    
    output_gdf = gpd.GeoDataFrame(output, geometry='geometry')
    output_gdf.to_csv(OUTPUT_FILE, index=False)