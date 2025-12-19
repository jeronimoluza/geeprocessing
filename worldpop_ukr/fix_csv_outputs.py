"""Script to fix sex/age_group swaps in existing CSV outputs."""

from pathlib import Path
import pandas as pd


def fix_csv_swaps(csv_path: Path) -> None:
    """
    Fix rows where sex == "T" and age_group in ["F", "M"].
    Swap them to sex in ["F", "M"] and age_group == "T".
    
    Parameters
    ----------
    csv_path : Path
        Path to the CSV file to fix.
    """
    df = pd.read_csv(csv_path)
    
    mask = (df["sex"] == "T") & (df["age_group"].isin(["F", "M"]))
    
    if mask.sum() == 0:
        print(f"  No swaps needed in {csv_path.name}")
        return
    
    df.loc[mask, ["sex", "age_group"]] = df.loc[mask, ["age_group", "sex"]].values
    
    df.to_csv(csv_path, index=False)
    print(f"  Fixed {mask.sum()} rows in {csv_path.name}")


def main():
    """Fix all CSV files in the outputs folder."""
    outputs_dir = Path(__file__).parent / "data" / "outputs"
    
    if not outputs_dir.exists():
        print(f"Outputs directory not found: {outputs_dir}")
        return
    
    csv_files = list(outputs_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {outputs_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process\n")
    
    for csv_path in sorted(csv_files):
        print(f"Processing {csv_path.name}...")
        fix_csv_swaps(csv_path)
    
    print("\nDone!")


if __name__ == "__main__":
    main()
