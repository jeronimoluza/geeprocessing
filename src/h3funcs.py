import h3
import geopandas as gpd
from shapely.geometry import shape

def h3_to_gdf(h3_input, crs="EPSG:4326"):
    """
    Converts one or multiple H3 hex IDs into a GeoDataFrame using h3.cells_to_geo.

    Parameters
    ----------
    h3_input : str or list of str
        A single H3 hex string or a list of H3 hex strings.
    crs : str, optional
        Coordinate Reference System for the GeoDataFrame (default is EPSG:4326).

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame with columns:
        - 'hex_id': H3 cell ID
        - 'geometry': shapely Polygon of the hex cell
    """
    # Ensure input is a list
    h3_list = [h3_input] if isinstance(h3_input, str) else h3_input

    records = []
    # Loop through each H3 cell
    for hex_id in h3_list:
        geojson = h3.cells_to_geo([hex_id])
        # Convert GeoJSON polygon to shapely geometry
        polygon = shape(geojson)
        records.append({"hex_id": hex_id, "geometry": polygon})

    return gpd.GeoDataFrame(records, crs=crs)
