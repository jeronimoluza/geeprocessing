import pandas as pd
import geopandas as gpd
from loading import load_geojson, load_hhs_data

HHS_DATA = '../data/hhs/l2idn_gps_b2.csv'
ROADS_DATA = '../data/roads/roads.geojson'
OUTPUT_FILE = '../outputs/roads/road_distances.csv'

if __name__ == "__main__":
    hhs_gdf = load_hhs_data(HHS_DATA)
    roads = load_geojson(ROADS_DATA)
    
    # Reproject to EPSG:3857 (Web Mercator) for accurate distance measurements in meters
    hhs_gdf = hhs_gdf.to_crs("EPSG:3857")
    roads = roads.to_crs("EPSG:3857")
    
    # Calculate distance from each HHS point to the nearest road
    hhs_gdf['distance_to_nearest_road_mts'] = hhs_gdf.geometry.apply(
        lambda point: roads.geometry.distance(point).min()
    )
    
    # Save results to CSV
    output_df = hhs_gdf.drop(columns=['geometry'])
    output_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"Distance calculations complete. Results saved to {OUTPUT_FILE}")
    print(f"Distance statistics (meters):")
    print(hhs_gdf['distance_to_nearest_road_mts'].describe())