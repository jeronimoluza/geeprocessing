"""This module contains functions for loading administrative boundaries and creating shapefiles needed for the main workflow."""
import geopandas as gpd
from pathlib import Path


def load_admin_area(level):
    """
    Load administrative boundary shapefile for Ukraine at a specified level.
    
    Parameters
    ----------
    level : int
        Administrative level (0-4). Level 0 is the country boundary,
        level 1 is regions, level 2 is districts, etc.
    
    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame containing the administrative boundaries.
    """
    shapefile_dir = Path(__file__).parent.parent / "data" / "shapefiles"
    shapefile_path = shapefile_dir / f"ukr_admbnda_adm{level}_sspe_20230201.shp"
    
    if not shapefile_path.exists():
        raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")
    
    return gpd.read_file(shapefile_path)


def create_adm3_geojson():
    """
    Create adm3.geojson containing ADM0_PCODE, ADM1_PCODE, ADM2_PCODE, ADM3_PCODE and geometry.
    """
    adm3 = load_admin_area(3)
    adm3_output = adm3[["ADM0_PCODE", "ADM1_PCODE", "ADM2_PCODE", "ADM3_PCODE", "geometry"]].copy()
    
    output_dir = Path(__file__).parent.parent / "data" / "aoi"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "adm3.geojson"
    adm3_output.to_file(output_path, driver="GeoJSON")
    print(f"Created {output_path}")
    
    return adm3_output


def create_adm4_w_outskirts_geojson():
    """
    Create adm4_w_outskirts.geojson by:
    1. Loading adm3 and adm4 shapefiles
    2. For each ADM3_PCODE, calculating the difference (holes) between adm3 and adm4
    3. Adding outskirts as new rows with ADM4_PCODE = {ADM3_PCODE}_outskirts
    4. Concatenating with original adm4 data
    """
    adm3 = load_admin_area(3)
    adm4 = load_admin_area(4)
    
    adm4_w_outskirts_list = []
    
    for adm3_pcode in adm3["ADM3_PCODE"].unique():
        polygon_a3 = adm3[adm3["ADM3_PCODE"] == adm3_pcode]
        polygon_a4 = adm4[adm4["ADM3_PCODE"] == adm3_pcode]
        
        adm4_copy = polygon_a4[["ADM0_PCODE", "ADM1_PCODE", "ADM2_PCODE", "ADM3_PCODE", "ADM4_PCODE", "geometry"]].copy()
        adm4_copy["outskirt"] = 0
        adm4_w_outskirts_list.append(adm4_copy)
        
        if len(polygon_a4) > 0:
            outskirts = polygon_a3.overlay(polygon_a4, how="difference")
            
            if len(outskirts) > 0:
                outskirts_copy = outskirts[["ADM0_PCODE", "ADM1_PCODE", "ADM2_PCODE", "geometry"]].copy()
                outskirts_copy["ADM3_PCODE"] = adm3_pcode
                outskirts_copy["ADM4_PCODE"] = f"{adm3_pcode}_outskirts"
                outskirts_copy["outskirt"] = 1
                outskirts_copy = outskirts_copy[["ADM0_PCODE", "ADM1_PCODE", "ADM2_PCODE", "ADM3_PCODE", "ADM4_PCODE", "outskirt", "geometry"]]
                
                adm4_w_outskirts_list.append(outskirts_copy)
    
    adm4_w_outskirts = gpd.GeoDataFrame(
        gpd.pd.concat(adm4_w_outskirts_list, ignore_index=True),
        crs=adm4.crs
    )
    
    output_dir = Path(__file__).parent.parent / "data" / "aoi"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "adm4_w_outskirts.geojson"
    adm4_w_outskirts.to_file(output_path, driver="GeoJSON")
    print(f"Created {output_path}")
    
    return adm4_w_outskirts

if __name__ == '__main__':
    print('Creating ADM3 GeoJSON...')
    create_adm3_geojson()
    print('Creating ADM4 with outskirts GeoJSON...')
    create_adm4_w_outskirts_geojson()
