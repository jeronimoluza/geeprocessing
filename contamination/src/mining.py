import geopandas as gpd
import pandas as pd
from loading import load_geojson, load_hhs_data
import os

HHS_DATA = '../data/hhs/l2idn_gps_b2.csv'
MINING_DATA = '../data/mining/mining.geojson'
OUTPUT_FILE = '../outputs/mining/mining_distances.csv'

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

if __name__ == "__main__":
    hhs_gdf = load_hhs_data(HHS_DATA)
    mining = load_geojson(MINING_DATA)

    # Reproject both to EPSG:3857 for buffer analysis
    hhs_gdf = hhs_gdf.to_crs('EPSG:3857')
    mining = mining.to_crs('EPSG:3857')
    
    # Get unique categories in mining data
    categories = mining['category'].unique()
    buffer_distances = [1000, 3000, 5000, 10000]  # in meters
    
    # For each buffer distance and category, count features within buffer
    for buffer_m in buffer_distances:
        buffer_km = buffer_m / 1000
        for category in categories:
            col_name = f'count_{category}_within_{buffer_km:.0f}km'
            
            # Create buffers around HHS points
            hhs_buffers = hhs_gdf.geometry.buffer(buffer_m)
            
            # Filter mining data by category
            mining_category = mining[mining['category'] == category]
            
            # Count mining features within each buffer
            counts = []
            for buffer_geom in hhs_buffers:
                count = mining_category.geometry.within(buffer_geom).sum()
                counts.append(count)
            
            hhs_gdf[col_name] = counts
    
    # Calculate distance to closest mining feature for each category
    for category in categories:
        col_name = f'distance_to_closest_{category}_km'
        mining_category = mining[mining['category'] == category]
        
        distances = []
        for hhs_point in hhs_gdf.geometry:
            if len(mining_category) > 0:
                min_distance = mining_category.geometry.distance(hhs_point).min()
                distances.append(min_distance / 1000)  # Convert to km
            else:
                distances.append(None)
        
        hhs_gdf[col_name] = distances
    
    # Reproject back to EPSG:4326 for export
    hhs_gdf = hhs_gdf.to_crs('EPSG:4326')
    
    # Export to CSV
    hhs_gdf.to_csv(OUTPUT_FILE, index=False)