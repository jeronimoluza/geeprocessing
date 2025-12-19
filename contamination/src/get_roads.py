from loading import load_hhs_data
import geopandas as gpd
HHS_DATA = '../data/hhs/l2idn_gps_b2.csv'
ROADS_DATA = '../data/roads/heigit_idn_roadsurface_lines.geojson'
ROADS_OUTPUT = '../data/roads/roads.geojson'

if __name__ == "__main__":

    print('Loading HHS data...')
    hhs_gdf = load_hhs_data(HHS_DATA)

    hhs_gdf['geometry'] = hhs_gdf.to_crs(epsg=3857).buffer(1000).to_crs(epsg=4326)
    filt = hhs_gdf.geometry.union_all()
    print('Loading and filtering roads data...')
    roads = gpd.read_file(ROADS_DATA, mask=filt)
    
    print('Saving roads...')
    roads.to_file(ROADS_OUTPUT, driver='GeoJSON')
    print('Done.')
