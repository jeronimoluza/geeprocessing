import pandas as pd
import geopandas as gpd
from loading import load_geojson, load_hhs_data

HHS_DATA = '../data/hhs/l2idn_gps_b2.csv'
HEALTHCARE_DATA = '../data/health/healthcare.geojson'
OUTPUT_FILE = '../outputs/health/health_distances.csv'

if __name__ == "__main__":
    hhs_gdf = load_hhs_data(HHS_DATA)
    healthcare = load_geojson(HEALTHCARE_DATA)
    healthcare = healthcare[healthcare.amenity.isin(['hospital', 'clinic'])]
    
    # Reproject both GeoDataFrames to EPSG:3857 for accurate distance calculations
    hhs_gdf = hhs_gdf.to_crs('EPSG:3857')
    healthcare = healthcare.to_crs('EPSG:3857')
    
    # Separate hospitals and clinics
    hospitals = healthcare[healthcare.amenity == 'hospital']
    clinics = healthcare[healthcare.amenity == 'clinic']
    
    # Calculate distance to nearest hospital (in km)
    hhs_gdf['distance_to_hospital_km'] = hhs_gdf.geometry.apply(
        lambda point: hospitals.geometry.distance(point).min() / 1000 if len(hospitals) > 0 else None
    )
    
    # Calculate distance to nearest clinic (in km)
    hhs_gdf['distance_to_clinic_km'] = hhs_gdf.geometry.apply(
        lambda point: clinics.geometry.distance(point).min() / 1000 if len(clinics) > 0 else None
    )
    
    hhs_gdf = hhs_gdf.to_crs('EPSG:4326')
    # Export to CSV
    hhs_gdf.to_csv(OUTPUT_FILE, index=False)
