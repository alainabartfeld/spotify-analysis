#%%
import logging
import json
import pandas as pd

#%%
def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
def get_csvs(DATA_DIR, OUTPUT_DIR, csv_subpath):
    # Find matching files
    logging.info("Grabbing streaming history")

    pattern = "StreamingHistory_music*.json"
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


def make_combined_csv(OUTPUT_DIR, combined_file_name):
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

    # Parse and sort by endTime
    df["endTime"] = pd.to_datetime(df["endTime"], errors="coerce")
    df = df.sort_values("endTime").reset_index(drop=True)

    # Write combined CSV
    df.to_csv(OUTPUT_FILE, index=False)

    # Delete individual CSVs
    for f in csv_files:
        f.unlink()

    logging.info(f"Created {OUTPUT_DIR}\{OUTPUT_FILE.name}, sorted by endTime, and deleted {len(csv_files)} CSV files.")
    # returns nothing, just writes the combined CSV
