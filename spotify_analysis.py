#%%
import logging
import os
from pathlib import Path
import my_utils

#%%
my_utils.setup_logging("spotify_analysis.log")
logging.info("Starting Spotify Analysis Pipeline")

# Setting paths up
json_subpath = "spotify_account_data_dec_2025"
csv_subpath = "streaming_history_data"
combined_file_name = "streaming_history_combined.csv"

DATA_DIR = Path(os.path.join(os.path.dirname(__file__), json_subpath)) 

OUTPUT_DIR = Path(os.path.join(os.path.dirname(__file__), csv_subpath))
OUTPUT_DIR.mkdir(exist_ok=True)

#%%
my_utils.get_csvs(DATA_DIR, OUTPUT_DIR, csv_subpath)
my_utils.make_combined_csv(OUTPUT_DIR, combined_file_name)
