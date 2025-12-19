from pathlib import Path
from src.download import download_worldpop_data, extract_worldpop_zip
from src.shapes import load_admin_area
from src.rasters import open_worldpop_raster, clip_raster_to_boundary


def main():
    year = 2020
    
    project_root = Path(__file__).parent
    data_dir = project_root / "data"
    worldpop_dir = data_dir / "worldpop" / str(year)
    output_dir = data_dir / "outputs"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Processing WorldPop data for {year}")
    print(f"{'='*60}\n")
    
    print("Step 1: Download WorldPop data")
    zip_file = download_worldpop_data(year, output_dir=worldpop_dir)
    
    print("\nStep 2: Extract downloaded data")
    extract_dir = extract_worldpop_zip(zip_file, extract_dir=worldpop_dir / "extracted")
    
    print("\nStep 3: Check for existing clipped data")
    existing_clipped = list(output_dir.glob(f"*{year}*_clipped.tif"))
    if existing_clipped:
        print(f"Clipped data for year {year} already exists:")
        for clipped in existing_clipped:
            print(f"  - {clipped}")
        print("Skipping processing.")
        return
    
    print("\nStep 4: Load administrative boundaries (ADM3)")
    admin_boundary = load_admin_area(level=3)
    print(f"  Loaded {len(admin_boundary)} administrative units")
    
    print("\nStep 5: Find and open raster files")
    raster_files = list(extract_dir.glob("*.tif"))
    if not raster_files:
        print(f"  Warning: No .tif files found in {extract_dir}")
        return
    
    print(f"  Found {len(raster_files)} raster file(s)")
    
    for raster_file in raster_files:
        print(f"\nStep 6: Processing {raster_file.name}")
        raster_data = open_worldpop_raster(str(raster_file))
        
        print("\nStep 7: Clip raster to administrative boundary")
        clipped_data = clip_raster_to_boundary(raster_data, admin_boundary)
        
        output_file = output_dir / f"{raster_file.stem}_clipped.tif"
        print(f"\nStep 8: Save clipped data to {output_file}")
        clipped_data.rio.to_raster(str(output_file))
        print(f"  Saved: {output_file}")
    
    print(f"\n{'='*60}")
    print("Processing complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
