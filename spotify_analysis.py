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

# where to put the combined file (I want to put them in the same place)
csv_subpath = "streaming_history_data"

# quirks btwn limited data and full history
pattern = [
    "StreamingHistory_music*.json"
    ,"Streaming_History_Audio*"
    ]

end_time_col = [
    "endTime"
    ,"ts"
    ]

ms_played_col = [
    "msPlayed"
    ,"ms_played"
]

# indexes json_subpath and combined_file_name
file_index_dict = {
    "my subset of data":0
    ,"my full listening history":1
    ,"Solomon full listening history":2
    }

# indexes pattern, end_time_col, and ms_played_col
pattern_end_col_and_ms_played_index_dict = {
    "subset of data":0
    ,"full listening history":1
}

file_index_level = file_index_dict["Solomon full listening history"]
pattern_end_col_and_ms_played_index_level = pattern_end_col_and_ms_played_index_dict["full listening history"]

#%%
# Setting paths up
DATA_DIR = my_utils.init_paths(json_subpath[file_index_level],csv_subpath)["DATA_DIR"]
OUTPUT_DIR = my_utils.init_paths(json_subpath[file_index_level],csv_subpath)["OUTPUT_DIR"]

#%%
my_utils.get_csvs(DATA_DIR, OUTPUT_DIR, csv_subpath, pattern[pattern_end_col_and_ms_played_index_level])
my_utils.make_combined_csv(OUTPUT_DIR, combined_file_name[file_index_level], end_time_col[pattern_end_col_and_ms_played_index_level],pattern[pattern_end_col_and_ms_played_index_level])

#%%
data = duckdb.read_csv(Path(os.path.join(OUTPUT_DIR,combined_file_name[file_index_level])))

#%%
# Clean up the house
ms_to_mins = my_utils.ms_to_mins
mins_to_hrs = my_utils.mins_to_hrs

base = duckdb.sql('''
        SELECT *
        FROM data
    '''
)

#%%
staging = duckdb.sql(f'''
        SELECT 
            {end_time_col[pattern_end_col_and_ms_played_index_level]} AS end_time
            ,platform
            ,ROUND({ms_played_col[pattern_end_col_and_ms_played_index_level]} / {ms_to_mins},2) AS mins_played
            ,ROUND(mins_played / {mins_to_hrs},2) AS hrs_played
            ,CASE
                WHEN LENGTH(CAST(month({end_time_col[pattern_end_col_and_ms_played_index_level]}) AS VARCHAR))=1 THEN concat(year({end_time_col[pattern_end_col_and_ms_played_index_level]}), '-0', month({end_time_col[pattern_end_col_and_ms_played_index_level]}))
                ELSE concat(year({end_time_col[pattern_end_col_and_ms_played_index_level]}), '-',  CAST(month({end_time_col[pattern_end_col_and_ms_played_index_level]}) AS VARCHAR))
            END AS end_time_yyyy_mm
            ,date({end_time_col[pattern_end_col_and_ms_played_index_level]}) AS end_time_date
            ,conn_country
            ,ip_addr
            ,master_metadata_track_name AS track_name
            ,master_metadata_album_artist_name AS artist_name
            ,master_metadata_album_album_name AS album_name
            ,spotify_track_uri
            ,episode_name
            ,episode_show_name
            ,spotify_episode_uri
            ,audiobook_title
            ,audiobook_uri
            ,audiobook_chapter_uri
            ,audiobook_chapter_title
            ,reason_start
            ,reason_end
            ,shuffle
            ,skipped
            ,offline
            ,offline_timestamp
            ,incognito_mode
        FROM base
    '''
)

songs_only = duckdb.sql(f'''
        SELECT *
        FROM staging
        WHERE track_name IS NOT NULL
    '''
)

podcasts_only = duckdb.sql(f'''
        SELECT *
        FROM staging
        WHERE episode_name IS NOT NULL
    '''
)

#%%
# Time period per year
duckdb.sql(f'''
            SELECT year(end_time) AS year
                ,MIN(end_time_date)
                ,MAX(end_time_date)
            FROM songs_only
            GROUP BY ALL
            ORDER BY year(end_time)
    '''
)

# %%
# Top x artists per year
x = 5
top_artists = duckdb.sql(f'''
    WITH ranked AS (
        SELECT
            year(end_time) AS year
            ,artist_name
            ,ROUND(SUM(mins_played), 2) AS mins_played
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY SUM(mins_played) DESC
            ) AS rnk
        FROM songs_only
        GROUP BY
            year(end_time)
            ,artist_name
    )
    SELECT
        year
        ,rnk
        ,artist_name
        ,mins_played
    FROM ranked
    WHERE rnk <= {x}
    ORDER BY year, rnk
''')

top_artists.to_csv("top_artists.csv")

#%%
# Top x artists across all listening history
duckdb.sql(f'''
        SELECT
            artist_name
            ,ROUND(SUM(mins_played), 2) AS mins_played
        FROM songs_only
        GROUP BY ALL
        ORDER BY SUM(mins_played) DESC
        LIMIT {x}
''')

# %%
# Top y songs per year based on number of streams
y = 5
top_songs = duckdb.sql(f'''
    WITH ranked AS (
        SELECT
            year(end_time) AS year
            ,track_name
            ,artist_name
            ,ROUND(SUM(mins_played), 2) AS mins_played
            ,COUNT(*) AS number_of_streams
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY COUNT(*) DESC
            ) AS rnk
        FROM songs_only
        GROUP BY
            year(end_time)
            ,track_name
            ,artist_name
    )
    SELECT
        year
        ,rnk
        ,track_name
        ,artist_name
        ,mins_played
        ,number_of_streams
    FROM ranked
    WHERE rnk <= {y}
    ORDER BY year, rnk
''')

top_songs.to_csv("top_songs.csv")

# %%
# Top z albums per year
z = 5
duckdb.sql(f'''
    WITH ranked AS (
        SELECT
            year(end_time) AS year
            ,album_name
            ,artist_name
            ,ROUND(SUM(mins_played), 2) AS mins_played
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY SUM(mins_played) DESC
            ) AS rnk
        FROM songs_only
        GROUP BY
            year(end_time)
            ,album_name
            ,artist_name
    )
    SELECT
        year
        ,rnk
        ,album_name
        ,mins_played
    FROM ranked
    WHERE rnk <= {z}
    ORDER BY year, rnk
''')

# %%
# Number of hours listened to per year
# Avg number of hours listened to per day
duckdb.sql(f'''
            SELECT DISTINCT year(end_time) AS year
                ,ROUND(SUM(hrs_played),2) AS hrs_played
                ,ROUND(SUM(hrs_played)/365,2) AS daily_avg_hrs_played
            FROM songs_only
            GROUP BY ALL
            ORDER BY year(end_time)
    '''
)

#%%
# Number of distinct songs/artists per year
duckdb.sql(f'''
            SELECT DISTINCT year(end_time) AS year
                ,COUNT(DISTINCT(artist_name)) AS number_of_artists
                ,COUNT(DISTINCT(track_name)) AS number_of_tracks
            FROM songs_only
            GROUP BY ALL
            ORDER BY year(end_time)
    '''
)

#%%
# Distribution of mins listened to per country
duckdb.sql(f'''
            SELECT DISTINCT conn_country
                ,ROUND(SUM(hrs_played),2) AS hrs_played
            FROM songs_only
            GROUP BY ALL
            ORDER BY ROUND(SUM(hrs_played),2) DESC
    '''
)

#%%
# What is ZZ country code
zz = duckdb.sql(f'''
            SELECT *
            FROM staging
            WHERE conn_country = 'ZZ'
    '''
)

# %%
# Top songs across all listening history
duckdb.sql(f'''
        SELECT
            track_name
            ,artist_name
            ,COUNT(*) AS number_of_times_played
        FROM songs_only
        GROUP BY ALL
        ORDER BY COUNT(*) DESC
        LIMIT {x}
''')


#%%
# Average hours listened to per day across all time
duckdb.sql(f'''
        SELECT 
            ROUND(
                (
                SUM(hrs_played)
               /
                -- get the # of days in the data set
                count(distinct(end_time_date))
                )
            ,2) AS daily_avg_hrs_played
        FROM songs_only
    '''
)

#%%
# Hours listened per year
duckdb.sql(f'''
        SELECT year(end_time) AS year
            ,ROUND(SUM(hrs_played),2)
        FROM songs_only
        GROUP BY ALL
        ORDER BY year(end_time)
    '''
)

#%%
# Top 20 listening days across all time
duckdb.sql(f'''
        SELECT DISTINCT end_time_date
            ,ROUND(SUM(hrs_played),2) AS total_hrs_played
            ,ROUND(SUM(mins_played),2) AS total_mins_played
        FROM songs_only
        GROUP BY end_time_date
        ORDER BY SUM(hrs_played) DESC
        LIMIT 20
    '''
)

# %%
# Number of mins/hours of podcasts per year
    # 2025 = 1289 mins (BAES #1)

# How often do I shuffle or skip?