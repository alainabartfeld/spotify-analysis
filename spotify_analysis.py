#%%
import logging
import os
from pathlib import Path
import my_utils
import duckdb

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

#%%
data = duckdb.read_csv(Path(os.path.join(OUTPUT_DIR,combined_file_name)))

#%%
# Clean up the house
ms_to_mins = 60000

base = duckdb.sql('''
        SELECT *
        FROM data
    '''
)

staging = duckdb.sql(f'''
        SELECT endTime AS end_time
            ,artistName AS artist_name
            ,trackName AS track_name
            ,round(msPlayed / {ms_to_mins},2) AS mins_played
        FROM data
    '''
)

streaming_history_2025 = duckdb.sql('''
        SELECT *
        FROM staging
        WHERE YEAR(end_time) = 2025
    '''
)

#%%
# Timeframe of 2025 data collected (which differs from the timeframe Wrapped pulls from)
duckdb.sql(f'''
        SELECT min(end_time), max(end_time)
        FROM streaming_history_2025
    '''
)

# %%
# Top x artists
x = 5
duckdb.sql(f'''
        SELECT artist_name, round(sum(mins_played),2) AS mins_played
        FROM streaming_history_2025
        GROUP BY artist_name
        ORDER BY sum(mins_played) DESC
        LIMIT {x}
    '''
)

# %%
# Top y songs
y = 20
duckdb.sql(f'''
            SELECT track_name, artist_name, count(*) AS number_of_streams, round(sum(mins_played),2) AS mins_played
            FROM streaming_history_2025
            GROUP BY track_name, artist_name
            ORDER BY count(*) DESC
            LIMIT {y}
    '''
)
# %%
