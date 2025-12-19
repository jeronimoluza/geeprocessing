"""This module contains functions for processing WorldPop rasters."""

import rioxarray
import xarray as xr
import geopandas as gpd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Tuple
from rasterstats import zonal_stats


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
    
    data = rioxarray.open_rasterio(raster_path, mask_and_scale=True)
    print(f"Opened raster: {raster_path}")
    
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


def sum_population_for_geometries(tif_path: str, geometries: gpd.GeoDataFrame) -> list:
    """
    Calculate zonal statistics (sum) for geometries using rasterstats.
    
    Parameters
    ----------
    tif_path : str
        Path to the raster TIF file.
    geometries : gpd.GeoDataFrame
        GeoDataFrame with geometries to calculate stats for.
    
    Returns
    -------
    list
        List of population sums, one per geometry.
    """
    try:
        stats = zonal_stats(
            geometries,
            tif_path,
            stats=['sum'],
            nodata=-99999., # Fill value on the rasters
            all_touched=False
        )
        
        pop_sums = [float(stat.get('sum', 0) or 0) for stat in stats]
        return pop_sums
    except Exception as e:
        print(f"Error calculating zonal stats for {tif_path}: {e}")
        return [0.0] * len(geometries)
