from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.download import download_worldpop_data
from src.file_managing import extract_worldpop_zip, delete_tif_file, get_tif_files
from src.rasters import open_worldpop_raster, parse_worldpop_filename, sum_population_for_geometry


YEARS = [2020, 2021, 2022]


def get_project_paths():
    """Get project directory paths."""
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    return {
        "project_root": project_root,
        "data_dir": data_dir,
        "worldpop_dir": data_dir / "worldpop",
        "output_dir": data_dir / "outputs",
        "aoi_dir": data_dir / "aoi",
    }


def load_geometries(paths: dict) -> tuple:
    """Load ADM3 and ADM4 with outskirts geometries."""
    adm3_path = paths["aoi_dir"] / "adm3.geojson"
    adm4_path = paths["aoi_dir"] / "adm4_w_outskirts.geojson"
    
    adm3 = gpd.read_file(adm3_path)
    adm4 = gpd.read_file(adm4_path)
    
    print(f"Loaded {len(adm3)} ADM3 geometries")
    print(f"Loaded {len(adm4)} ADM4 (with outskirts) geometries")
    
    return adm3, adm4


def get_output_csv_path(paths: dict, year: int, admin_level: str) -> Path:
    """Get output CSV path for a given year and admin level."""
    return paths["output_dir"] / f"worldpop_{year}_{admin_level}.csv"


def process_raster_for_geometries(
    raster_path: Path,
    geometries: gpd.GeoDataFrame,
    pcode_column: str,
) -> list:
    """
    Process a single raster file for all geometries.
    
    Returns list of dicts with pcode, sex, age_group, pop.
    """
    raster = open_worldpop_raster(str(raster_path))
    metadata = parse_worldpop_filename(raster_path.name)
    
    results = []
    crs = geometries.crs
    
    for idx, row in tqdm(geometries.iterrows(), total=len(geometries), desc=f"Processing geometries", leave=False):
        pcode = row[pcode_column]
        geometry = row.geometry
        
        pop_sum = sum_population_for_geometry(raster, geometry, crs)
        
        results.append({
            pcode_column.lower(): pcode,
            "sex": metadata["sex"],
            "age_group": metadata["age_group"],
            "pop": pop_sum,
        })
    
    raster.close()
    return results


def process_year_admin_level(
    year: int,
    paths: dict,
    geometries: gpd.GeoDataFrame,
    admin_level: str,
    pcode_column: str,
    delete_tifs: bool = True,
) -> None:
    """
    Process all WorldPop rasters for a year and admin level.
    
    Parameters
    ----------
    year : int
        Year to process.
    paths : dict
        Project paths.
    geometries : gpd.GeoDataFrame
        Geometries to process.
    admin_level : str
        Admin level name (adm3 or adm4).
    pcode_column : str
        Column name for pcode in geometries.
    delete_tifs : bool
        Whether to delete TIF files after processing.
    """
    output_csv = get_output_csv_path(paths, year, admin_level)
    
    if output_csv.exists():
        print(f"  Output already exists: {output_csv.name} - SKIPPING")
        return
    
    year_dir = paths["worldpop_dir"] / str(year)
    zip_file = download_worldpop_data(year, output_dir=year_dir)
    extract_dir = year_dir / "extracted"
    extract_worldpop_zip(zip_file, extract_dir=extract_dir)
    
    tif_files = get_tif_files(extract_dir)
    if not tif_files:
        print(f"  Warning: No TIF files found in {extract_dir}")
        return
    
    print(f"  Processing {len(tif_files)} raster files for {len(geometries)} geometries...")
    
    all_results = []
    
    for i, tif_path in enumerate(tif_files, 1):
        print(f"    [{i}/{len(tif_files)}] {tif_path.name}")
        
        results = process_raster_for_geometries(tif_path, geometries, pcode_column)
        all_results.extend(results)
        
        if delete_tifs:
            delete_tif_file(tif_path)
    
    df = pd.DataFrame(all_results)
    df["year"] = year
    
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"  Saved: {output_csv.name}")


def main():
    paths = get_project_paths()
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 60)
    print("WorldPop Ukraine Processing Pipeline")
    print("=" * 60)
    
    print("\nLoading geometries...")
    adm3, adm4 = load_geometries(paths)
    
    for year in YEARS:
        print(f"\n{'='*60}")
        print(f"Processing year: {year}")
        print("=" * 60)
        
        print(f"\n[ADM3] Processing {year}...")
        process_year_admin_level(
            year=year,
            paths=paths,
            geometries=adm3,
            admin_level="adm3",
            pcode_column="ADM3_PCODE",
            delete_tifs=False,
        )
        
        print(f"\n[ADM4] Processing {year}...")
        process_year_admin_level(
            year=year,
            paths=paths,
            geometries=adm4,
            admin_level="adm4",
            pcode_column="ADM4_PCODE",
            delete_tifs=True,
        )
    
    print(f"\n{'='*60}")
    print("Processing complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
