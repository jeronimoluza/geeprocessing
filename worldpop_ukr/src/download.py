"""This module contains the functions needed to download WorldPop data."""
import requests
from pathlib import Path
from typing import Optional


def download_worldpop_data(year: int, output_dir: Optional[str] = None) -> Path:
    """
    Download WorldPop Age and Sex Structures data for a specific year.
    
    Parameters
    ----------
    year : int
        Year to download data for (2015-2030).
    output_dir : str, optional
        Directory to save the downloaded file. If None, saves to data/worldpop.
    
    Returns
    -------
    Path
        Path to the downloaded zip file.
    
    Raises
    ------
    ValueError
        If year is outside the valid range (2015-2030).
    requests.RequestException
        If the download fails.
    """
    if not 2015 <= year <= 2030:
        raise ValueError(f"Year must be between 2015 and 2030, got {year}")
    
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / "data" / "worldpop"
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    url = f"https://data.worldpop.org/GIS/AgeSex_structures/Global_2015_2030/R2025A/{year}/UKR/v1/100m/ukr_agesex_structures_{year}_CN_100m_R2025A_v1.zip"
    
    output_file = output_dir / f"ukr_agesex_structures_{year}_CN_100m_R2025A_v1.zip"
    
    if output_file.exists():
        print(f"Zip file already exists: {output_file}")
        return output_file
    
    tif_files = list(output_dir.glob(f"**/*{year}*.tif"))
    if tif_files:
        print(f"TIF files for year {year} already exist:")
        for tif in tif_files:
            print(f"  - {tif}")
        return output_file
    
    print(f"Downloading WorldPop data for {year}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    percent = (downloaded / total_size) * 100
                    print(f"  Progress: {percent:.1f}%", end='\r')
    
    print(f"\nDownload complete: {output_file}")
    return output_file


