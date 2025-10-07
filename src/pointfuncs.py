import geopandas as gpd
from shapely.geometry import Point

def group_points(gdf: gpd.GeoDataFrame, group_cols: list) -> gpd.GeoDataFrame:
    """
    Group points in a GeoDataFrame by categorical columns and replace 
    each group's geometry with the centroid of the grouped points.
    
    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame containing Point geometries.
    group_cols : list
        List of column names to group by.
    
    Returns
    -------
    geopandas.GeoDataFrame
        A new GeoDataFrame with one row per group and centroid geometries.
    """
    if not all(gdf.geometry.geom_type == "Point"):
        raise ValueError("All geometries must be Point type.")
    
    # Compute centroid per group (mean of x and y)
    grouped = (
        gdf.groupby(group_cols, as_index=False)
        .agg({
            **{col: 'first' for col in gdf.columns if col not in group_cols + ['geometry']},
            'geometry': lambda geom: Point(geom.x.mean(), geom.y.mean())
        })
    )
    
    grouped = gpd.GeoDataFrame(grouped, geometry='geometry', crs=gdf.crs)
    return grouped
