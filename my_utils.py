#%%
import logging
import json
import os
from pathlib import Path
import pandas as pd
import duckdb

#%%
ms_to_mins = 60000
mins_to_hrs = 60

#%%
def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
def get_csvs(DATA_DIR, OUTPUT_DIR, csv_subpath, pattern):
    # Find matching files
    logging.info("Grabbing streaming history")

    pattern = pattern
    json_files = DATA_DIR.glob(pattern)
    json_file_count = sum(1 for _ in DATA_DIR.glob(pattern))

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Write CSV to new folder
        csv_file = OUTPUT_DIR / f"{json_file.stem}.csv"
        df.to_csv(csv_file, index=False)

        logging.info(f"Converted to CSV: {csv_subpath}/{csv_file.name}")

    logging.info(f"Converted {json_file_count} total JSON files to CSV's.")
    # returns nothing, just overwrites the JSON files as CSV's


def make_combined_csv(OUTPUT_DIR, combined_file_name, end_time_column, pattern):
    # pattern of individual files (not the combined one) to delete at the end
    pattern = pattern
    
    # Output file
    OUTPUT_FILE = OUTPUT_DIR / combined_file_name

    # Find all CSV files (exclude output file if re-running)
    csv_files = [
        f for f in OUTPUT_DIR.glob("*.csv")
        if f.name != OUTPUT_FILE.name
    ]

    if not csv_files:
        raise ValueError("No CSV files found to concatenate.")

    # Read & concatenate
    df = pd.concat(
        (pd.read_csv(f) for f in csv_files),
        ignore_index=True
    )

    # Parse and sort by end_time_column
    df[end_time_column] = pd.to_datetime(df[end_time_column], errors="coerce")
    df = df.sort_values(end_time_column).reset_index(drop=True)

    # Write combined CSV
    df.to_csv(OUTPUT_FILE, index=False)

    for f in OUTPUT_DIR.rglob(pattern):
        logging.info(f"Deleting individual files")
        f.unlink()

    logging.info(f"Created {OUTPUT_DIR}\{OUTPUT_FILE.name}, sorted by {end_time_column}, and deleted {len(csv_files)} CSV files.")
    # returns nothing, just writes the combined CSV

def init_paths(json_subpath, csv_subpath):
    DATA_DIR = Path(os.path.join(os.path.dirname(__file__), json_subpath)) 
    OUTPUT_DIR = Path(os.path.join(os.path.dirname(__file__), csv_subpath))
    OUTPUT_DIR.mkdir(exist_ok=True)

    return({
        "DATA_DIR": DATA_DIR
        ,"OUTPUT_DIR": OUTPUT_DIR
    })
    
def get_limited_streaming_as_staging(end_time_column):
    # function to query the limited data
    # requires "FROM data" to reference limited data and not the entire history
    return(
        duckdb.sql(f'''
        SELECT {end_time_column} AS end_time
            ,artistName AS artist_name
            ,trackName AS track_name
            ,ROUND(msPlayed / {ms_to_mins},2) AS mins_played
            ,ROUND(mins_played / {mins_to_hrs},2) AS hrs_played
            ,CASE
                WHEN LENGTH(CAST(month(end_time) AS VARCHAR))=1 THEN concat(year(end_time), '-0', month(end_time))
                ELSE concat(year(end_time), '-',  CAST(month(end_time) AS VARCHAR))
            END AS end_time_yyyy_mm
            ,date(endTime) AS end_time_date
        FROM data
        WHERE year({end_time_column}) = 2025
    '''
        )
)
