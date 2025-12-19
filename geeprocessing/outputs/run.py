import pandas as pd
import os
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description="Concatenate weather data CSV files.")
parser.add_argument("--c", required=True, help="ISO3 country code (e.g., KGZ)")
parser.add_argument("--start", type=int, required=True, help="Start year")
parser.add_argument("--end", type=int, required=True, help="End year")

args = parser.parse_args()

# Generate file list
files_to_concat = []
all_files = os.listdir()
for year in range(args.start, args.end + 1):
    # The file listing shows filenames like 'weather_hourly_TJK_2015_m01-01.csv'
    # We will filter based on the pattern for the given country code and year.
    pattern = f"weather_hourly_{args.c}_{year}"
    for f in all_files:
        if f.startswith(pattern) and f.endswith(".csv"):
            files_to_concat.append(f)

# Check if any files were found
if not files_to_concat:
    print(f"No files found for ISO3 '{args.c}' between {args.start} and {args.end}.")
    exit()

# Sort files to ensure chronological order before concatenation
files_to_concat.sort()

# Concatenate and save
output_filename = f"{args.c}_weather_hourly_{args.start}_{args.end}.csv"

# Read and concatenate files
df_list = [pd.read_csv(file) for file in files_to_concat]
concatenated_df = pd.concat(df_list, ignore_index=True)

# Drop the ".geo" column if it exists
if ".geo" in concatenated_df.columns:
    concatenated_df = concatenated_df.drop(".geo", axis=1)

concatenated_df.reset_index(drop=True).to_csv(output_filename, index=None)

print(f"Successfully concatenated {len(files_to_concat)} files into {output_filename}")
