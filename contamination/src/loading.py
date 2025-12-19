import pandas as pd
import geopandas
from shapely.geometry import Point
import os

def load_geojson(file_path: str) -> geopandas.GeoDataFrame:
    """
    Loads a GeoJSON file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    return geopandas.read_file(file_path)
    

def load_hhs_data(file_path: str) -> geopandas.GeoDataFrame:
    """
    Loads data from a CSV file, using latitude and longitude columns to create
    a GeoDataFrame of points.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame containing points created from
                                latitude and longitude, with a CRS of EPSG:4326.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    df = pd.read_csv(file_path)

    # Ensure latitude and longitude columns exist
    if "latitude" not in df.columns or "longitude" not in df.columns:
        raise ValueError(
            "CSV file must contain 'latitude' and 'longitude' columns."
        )

    # Create Point geometries
    geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]

    # Create GeoDataFrame
    gdf = geopandas.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    return gdf


if __name__ == "__main__":
    data_file_path = os.path.join(
        '..', "data", "hhs", "l2idn_gps_b2.csv"
    )

    try:
        hhs_gdf = load_hhs_data(data_file_path)
        print(f"Successfully loaded GeoDataFrame with {len(hhs_gdf)} points.")
        print(hhs_gdf.head())
        hhs_gdf.to_csv("test.csv")
    except FileNotFoundError as e:
        print(e)
    except ValueError as e:
        print(e)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
