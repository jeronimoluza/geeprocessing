"""This module contains functions for processing WorldPop rasters."""

import rioxarray
import xarray as xr
import geopandas as gpd
from pathlib import Path
from typing import Dict, Any


def open_worldpop_raster(raster_path: str) -> xr.Dataset:
    """
    Open a WorldPop raster file using rioxarray.
    
    Parameters
    ----------
    raster_path : str
        Path to the raster file (typically .tif).
    
    Returns
    -------
    xr.Dataset
        Xarray Dataset with the raster data.
    """
    raster_path = Path(raster_path)
    
    if not raster_path.exists():
        raise FileNotFoundError(f"Raster file not found: {raster_path}")
    
    data = rioxarray.open_rasterio(raster_path)
    print(f"Opened raster: {raster_path}")
    print(f"  Shape: {data.shape}")
    print(f"  CRS: {data.rio.crs}")
    
    return data


def clip_raster_to_boundary(raster: xr.DataArray, boundary: gpd.GeoDataFrame) -> xr.DataArray:
    """
    Clip a raster to an administrative boundary.
    
    Parameters
    ----------
    raster : xr.DataArray
        The raster data to clip.
    boundary : gpd.GeoDataFrame
        GeoDataFrame containing the boundary geometry.
    
    Returns
    -------
    xr.DataArray
        Clipped raster data.
    """
    clipped = raster.rio.clip(boundary.geometry, boundary.crs)
    print(f"Clipped raster to boundary")
    return clipped


def compute_raster_stats(raster: xr.DataArray, admin_boundary: gpd.GeoDataFrame) -> Dict[str, Any]:
    """
    Compute raster statistics within administrative boundaries.
    
    Parameters
    ----------
    raster : xr.DataArray
        The raster data.
    admin_boundary : gpd.GeoDataFrame
        GeoDataFrame with administrative boundaries.
    
    Returns
    -------
    dict
        Dictionary containing raster statistics (placeholder for now).
    """
    stats = {
        'mean': None,
        'sum': None,
        'min': None,
        'max': None,
        'std': None,
    }
    
    print("Computing raster statistics...")
    
    return stats
