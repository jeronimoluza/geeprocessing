"""This module contains functions for unzipping WorldPop files and deleting them after processing."""
import zipfile
from pathlib import Path
from typing import Optional, List


def extract_worldpop_zip(zip_path: str, extract_dir: Optional[str] = None) -> Path:
    """
    Extract WorldPop zip file to a directory.
    
    Parameters
    ----------
    zip_path : str
        Path to the WorldPop zip file.
    extract_dir : str, optional
        Directory to extract to. If None, extracts to same directory as zip.
    
    Returns
    -------
    Path
        Path to the extraction directory.
    """
    zip_path = Path(zip_path)
    
    if extract_dir is None:
        extract_dir = zip_path.parent / zip_path.stem
    else:
        extract_dir = Path(extract_dir)
    
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    existing_tifs = list(extract_dir.glob("*.tif"))
    if existing_tifs:
        print(f"TIF files already extracted in: {extract_dir}")
        return extract_dir
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    print(f"Extracted to: {extract_dir}")
    return extract_dir


def delete_tif_file(tif_path: Path) -> None:
    """
    Delete a TIF file after processing.
    
    Parameters
    ----------
    tif_path : Path
        Path to the TIF file to delete.
    """
    tif_path = Path(tif_path)
    if tif_path.exists():
        tif_path.unlink()
        print(f"Deleted: {tif_path.name}")


def get_tif_files(extract_dir: Path) -> List[Path]:
    """
    Get list of TIF files in a directory.
    
    Parameters
    ----------
    extract_dir : Path
        Directory containing TIF files.
    
    Returns
    -------
    List[Path]
        List of paths to TIF files.
    """
    return sorted(Path(extract_dir).glob("*.tif"))
