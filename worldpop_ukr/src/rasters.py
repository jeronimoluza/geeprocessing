"""This module contains functions for processing WorldPop rasters."""

import rioxarray
import xarray as xr
import geopandas as gpd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Tuple


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


def parse_worldpop_filename(filename: str) -> Dict[str, str]:
    """
    Parse WorldPop filename to extract metadata.
    
    Example: ukr_f_00_2020_CN_100m_R2025A_v1.tif
    
    Parameters
    ----------
    filename : str
        WorldPop filename.
    
    Returns
    -------
    dict
        Dictionary with keys: iso, sex, age_group, year
    """
    stem = Path(filename).stem
    parts = stem.split("_")
    
    return {
        "iso": parts[0],
        "sex": parts[1],
        "age_group": parts[2],
        "year": parts[3],
    }


def sum_population_for_geometry(raster: xr.DataArray, geometry, crs) -> float:
    """
    Clip raster to a single geometry and sum the population values.
    
    Parameters
    ----------
    raster : xr.DataArray
        The raster data.
    geometry : shapely.geometry
        Single geometry to clip to.
    crs : CRS
        Coordinate reference system.
    
    Returns
    -------
    float
        Sum of population values within the geometry.
    """
    try:
        clipped = raster.rio.clip([geometry], crs=crs, drop=True, all_touched=True)
        values = clipped.values
        valid_values = values[~np.isnan(values) & (values >= 0)]
        return float(np.sum(valid_values))
    except Exception:
        return 0.0
