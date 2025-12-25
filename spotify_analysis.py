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
mins_to_hrs = 60

base = duckdb.sql('''
        SELECT *
        FROM data
    '''
)

staging = duckdb.sql(f'''
        SELECT endTime AS end_time
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
        SELECT artist_name, ROUND(SUM(mins_played),2) AS mins_played
        FROM streaming_history_2025
        GROUP BY artist_name
        ORDER BY SUM(mins_played) DESC
        LIMIT {x}
    '''
)

# %%
# Top y songs
y = 20
duckdb.sql(f'''
            SELECT track_name, artist_name, COUNT(*) AS number_of_streams, ROUND(SUM(mins_played),2) AS mins_played
            FROM streaming_history_2025
            GROUP BY track_name, artist_name
            ORDER BY COUNT(*) DESC
            LIMIT {y}
    '''
)

# %%
# Streaming by month and grand total
duckdb.sql(f'''
           WITH monthly AS (
            SELECT DISTINCT end_time_yyyy_mm
                , ROUND(SUM(mins_played)/60,2) AS hrs_played
            FROM streaming_history_2025
            GROUP BY ALL
            ORDER BY end_time_yyyy_mm
        )
        , total AS (
            SELECT 'Grand total', ROUND(SUM(mins_played)/60,2) AS hrs_played
            FROM streaming_history_2025
        )
        SELECT * FROM monthly
        UNION ALL
        SELECT * FROM total
    '''
)

#%%
# Average hours listened to per day
duckdb.sql(f'''
        SELECT ROUND(
                (
                SUM(hrs_played)
                /
                -- get the # of days in the time period by casting the interval as a string, parsing it for the #, then casting it as an integer
                CAST(CAST(max(end_time)-min(end_time) AS VARCHAR)[1:4] AS INT)
                )
            ,2) AS daily_avg_hrs_played
        FROM streaming_history_2025
    '''
)

#%%
# Top z listening days
z = 5
duckdb.sql(f'''
        SELECT DISTINCT end_time_date, ROUND(SUM(hrs_played),2) AS total_hrs_played
        FROM streaming_history_2025
        GROUP BY end_time_date
        ORDER BY SUM(hrs_played) DESC
        LIMIT {z}
    '''
)

# %%
