import geopandas as gpd
import numpy as np
from shapely.geometry import Point


def group_points(gdf: gpd.GeoDataFrame, group_cols: list) -> gpd.GeoDataFrame:
    """
    Group points in a GeoDataFrame by categorical columns and replace
    each group's geometry with the centroid (mean X/Y) of its points.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame containing Point geometries.
    group_cols : list
        List of column names to group by.

    Returns
    -------
    geopandas.GeoDataFrame
        One row per group with centroid geometries.
    """
    if not all(gdf.geometry.geom_type == "Point"):
        raise ValueError("All geometries must be Point type.")

    # Work in projected CRS for accurate mean calculations
    gdf_3857 = gdf.to_crs(3857)

    grouped = gdf_3857.groupby(group_cols, as_index=False).agg(
        {
            **{
                col: "first"
                for col in gdf.columns
                if col not in group_cols + ["geometry"]
            },
            "geometry": lambda geom: Point(
                np.mean([p.x for p in geom]), np.mean([p.y for p in geom])
            ),
        }
    )

    # Convert back to original CRS
    grouped = gpd.GeoDataFrame(grouped, geometry="geometry", crs=3857).to_crs(
        gdf.crs
    )

    return grouped


if __name__ == "__main__":
    import pandas as pd

    points = pd.read_csv("../data/l2kgz_gps.csv")
    points["geometry"] = points.apply(
        lambda row: Point(row["gps_lon"], row["gps_lat"]), axis=1
    )
    points = gpd.GeoDataFrame(points, crs=4326).to_crs(3857)
    grouped = (
        group_points(points, ["aiyl"])
        .drop(["gps_lat", "gps_lon"], axis=1)
        .to_crs(4326)
    )
    grouped.to_file("../data/grouped.geojson", driver="GeoJSON")
