#%%
import logging
import os
from pathlib import Path
import my_utils
import duckdb

#%%
my_utils.setup_logging("spotify_analysis.log")
logging.info("Starting Spotify Analysis Pipeline")

#%%
# Set stuff up to consider querying from limited listening history or full listening history
# 0 index is limited listening history (Dec 2024 - Dec 2025)
# 1 index is full listening history up until Christmas Day 2025
# 2 index is Solomon's full listening history up until Christmas Day 2025

# where the JSONs are located
json_subpath = [
    "spotify_account_data_dec_2025"
    ,"spotify_extended_streaming_history"
    ,"solomon_spotify_extended_streaming_history"
    ]

# what to call the combined file
combined_file_name = [
    "dec_2025_streaming_history_combined.csv"
    ,"spotify_extended_streaming_history_combined.csv"
    ,"solomon_spotify_extended_streaming_history_combined.csv"
    ]

pattern = [
    "StreamingHistory_music*.json"
    ,"Streaming_History_Audio*"
    ]

end_time_col = [
    "endTime"
    ,"ts"
    ]

# where to put the combined file (I want to put them in the same place)
csv_subpath = "streaming_history_data"

# indexes json_subpath and combined_file_name
file_index_dict = {
    "my subset of data":0
    ,"my full listening history":1
    ,"Solomon full listening history":2
    }

# indexes pattern and end_time_col
pattern_and_end_col_index_dict = {
    "subset of data":0
    ,"full listening history":1
}

file_index_level = file_index_dict["my full listening history"]
pattern_and_end_col_index_level = pattern_and_end_col_index_dict["full listening history"]

#%%
# Setting paths up
DATA_DIR = my_utils.init_paths(json_subpath[file_index_level],csv_subpath)["DATA_DIR"]

OUTPUT_DIR = my_utils.init_paths(json_subpath[file_index_level],csv_subpath)["OUTPUT_DIR"]

#%%
my_utils.get_csvs(DATA_DIR, OUTPUT_DIR, csv_subpath, pattern[pattern_and_end_col_index_level])
my_utils.make_combined_csv(OUTPUT_DIR, combined_file_name[file_index_level], end_time_col[pattern_and_end_col_index_level],pattern[pattern_and_end_col_index_level])

#%%
data = duckdb.read_csv(Path(os.path.join(OUTPUT_DIR,combined_file_name[file_index_level])))
