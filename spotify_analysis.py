#%%
import logging
import os
from pathlib import Path
import my_utils
import duckdb
from jinja2 import Template

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

file_index_level = file_index_dict["my full listening history"]
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
            -- standardize the platform
            ,CASE
                WHEN LOWER(platform) LIKE '%ios%' THEN 'iOS'
                WHEN LOWER(platform) LIKE '%os%' THEN 'OS'
                WHEN LOWER(platform) LIKE '%windows%' THEN 'Windows'
                WHEN LOWER(platform) LIKE '%cast%' THEN 'Cast'                ELSE platform
            END AS platform_standard

            -- get a proxy for the listening device
            ,CASE
                WHEN CONTAINS(LOWER(platform), 'windows')
                    THEN 'PC'
                WHEN CONTAINS(platform,'(') AND NOT CONTAINS(LOWER(platform), 'windows')
                    THEN SPLIT(SPLIT(CAST(platform AS VARCHAR),'(')[2],')')[1]
                WHEN CONTAINS (platform, ';')
                    THEN SPLIT(CAST(platform AS VARCHAR),';')[2]
                ELSE platform
            END AS device

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
            
            -- add way to differentiate btwn song and podcast records
            ,CASE
                WHEN spotify_track_uri IS NOT NULL THEN 'song'
                WHEN spotify_episode_uri IS NOT NULL THEN 'podcast'
            END AS song_podcast_flag
            
            -- I have never listened to an audiobook but keep them commented in case that changes
            --,audiobook_title
            --,audiobook_uri
            --,audiobook_chapter_uri
            --,audiobook_chapter_title
            
            ,reason_start
            ,reason_end
            
            -- make these boolean columns summable
            ,shuffle
            ,CASE
                WHEN shuffle = 'TRUE' THEN 1
                ELSE 0
            END AS shuffle_binary
            
            ,skipped
            ,CASE
                WHEN skipped = 'TRUE' THEN 1
                ELSE 0
            END AS skipped_binary
            
            ,offline
            ,CASE
                WHEN offline = 'TRUE' THEN 1
                ELSE 0
            END AS offline_binary
            ,offline_timestamp
            
            ,incognito_mode
            ,CASE
                WHEN incognito_mode = 'TRUE' THEN 1
                ELSE 0
            END AS incognito_mode_binary
        FROM base
    '''
)

songs_only = duckdb.sql(f'''
        -- GRAIN = spotify_track_uri
        SELECT *
        FROM staging
        WHERE song_podcast_flag = 'song'
    '''
)

podcasts_only = duckdb.sql(f'''
        -- GRAIN = spotify_episode_uri
        SELECT *
        FROM staging
        WHERE song_podcast_flag = 'podcast'
    '''
)

# staging.to_csv("staging.csv")


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
            ,ROUND(SUM(hrs_played), 2) AS hrs_played
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY SUM(hrs_played) DESC
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
        ,hrs_played
    FROM ranked
    WHERE rnk <= {x}
    ORDER BY year, rnk
''')

# top_artists.to_csv("top_artists.csv")

#%%
# Top x artists across all listening history
duckdb.sql(f'''
        SELECT
            artist_name
            ,ROUND(SUM(hrs_played), 2) AS hrs_played
        FROM songs_only
        GROUP BY ALL
        ORDER BY SUM(hrs_played) DESC
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
            ,ROUND(SUM(hrs_played), 2) AS hrs_played
            ,COUNT(*) AS number_of_streams
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY COUNT(*) DESC
            ) AS rnk
        FROM songs_only
        GROUP BY
            year(end_time)
            ,spotify_track_uri
            ,track_name
            ,artist_name
    )
    SELECT
        year
        ,rnk
        ,track_name
        ,artist_name
        ,hrs_played
        ,number_of_streams
    FROM ranked
    WHERE rnk <= {y}
    ORDER BY year, rnk
''')

# top_songs.to_csv("top_songs.csv")


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
# Monthly listening in 2025
duckdb.sql(f'''
        WITH monthly AS (
            SELECT end_time_yyyy_mm
                ,ROUND(SUM(hrs_played),2) AS total_hrs_played
            FROM songs_only
            WHERE YEAR(end_time) = '2025'
            GROUP BY end_time_yyyy_mm
            ORDER BY end_time_yyyy_mm
        )
        , total AS (
            SELECT 'Grand total'
                ,ROUND(SUM(hrs_played),2) AS total_hrs_played
            FROM songs_only
            WHERE YEAR(end_time) = '2025'
        )
        SELECT * FROM monthly
        UNION ALL
        SELECT * FROM total
    '''
)

#%%
# # Avg number of hours listened to per day
duckdb.sql(f'''
            SELECT DISTINCT year(end_time) AS year
                ,ROUND(SUM(mins_played),2) AS mins_played
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

#%%
# What platform do I use most?
duckdb.sql(f'''
        SELECT platform_standard
            ,ROUND(SUM(hrs_played)) AS hrs_played
        FROM staging
        GROUP BY ALL
        ORDER BY hrs_played DESC
    '''
)

#%%
# Top devices
duckdb.sql(f'''
        SELECT device, ROUND(SUM(hrs_played),2) AS hrs_played
        FROM staging
        GROUP BY device
        ORDER BY hrs_played desc
    '''
)

# %%
# How often do I shuffle, skip, lisen offline, and listen incognito per year?
measures = [
    'shuffle_binary'
    ,'skipped_binary'
    ,'offline_binary'
    ,'incognito_mode_binary'
]

pct_sql_template = Template('''
            SELECT     
                YEAR(end_time) AS year,
                -- for all the columns, get the proportion, round it to 2 decimals, then add the percentage sign at the end
                {% for col in measures %}
                CONCAT(CAST(
                        ROUND(
                            (SUM({{ col }})/COUNT(*))*100,2)
                        AS VARCHAR
                    ),'%') 
                AS {{ col }}_pct
                    {% if not loop.last %}
                    ,
                    {% endif %}
                {% endfor %}
            FROM staging
            GROUP BY YEAR(end_time)
            ORDER BY YEAR(end_time)
        '''
    )
 
duckdb.sql(pct_sql_template.render(measures=measures))

# %%
# Top a podcasts per year
a = 5
top_podcasts = duckdb.sql(f'''
    WITH ranked AS (
        SELECT
            year(end_time) AS year
            ,episode_show_name
            ,ROUND(SUM(hrs_played), 2) AS hrs_played
            ,RANK() OVER (
                PARTITION BY year(end_time)
                ORDER BY SUM(hrs_played) DESC
            ) AS rnk
        FROM podcasts_only
        GROUP BY
            year(end_time)
            ,episode_show_name
    )
    SELECT
        year
        ,rnk
        ,episode_show_name
        ,hrs_played
    FROM ranked
    WHERE rnk <= {a}
    ORDER BY year, rnk
''')

# top_podcasts.to_csv("top_podcasts.csv")

# %%
# Number of mins/hours of podcasts per year
duckdb.sql(f'''
            SELECT YEAR(end_time) AS year
                ,ROUND(SUM(hrs_played),2) AS hrs_played
                ,ROUND(SUM(mins_played),2) AS mins_played
            FROM podcasts_only
            GROUP BY year,song_podcast_flag
            ORDER BY year
    '''
)

# %%
# What percentage of podcasts vs. music have I listened to and how has that changed over time?
# Would be interesting to incorporate Libby listening data to this
duckdb.sql(f'''
        WITH pods AS (
            SELECT YEAR(end_time) AS year
                ,ROUND(SUM(hrs_played),2) AS hrs_played
                ,ROUND(SUM(mins_played),2) AS mins_played
            FROM podcasts_only
            GROUP BY year
        )
        ,music AS (
            SELECT YEAR(end_time) AS year
                ,ROUND(SUM(hrs_played),2) AS hrs_played
                ,ROUND(SUM(mins_played),2) AS mins_played
            FROM songs_only
            GROUP BY year
        )
        ,total AS (
            SELECT YEAR(end_time) AS year
                ,ROUND(SUM(hrs_played),2) AS hrs_played
                ,ROUND(SUM(mins_played),2) AS mins_played
            FROM staging
            GROUP BY year
        )
        ,joined AS (
            SELECT pods.year
                ,pods.hrs_played AS pod_hrs
                -- ,pods.mins_played AS pod_mins
                ,music.hrs_played AS music_hrs
                -- ,music.mins_played AS music_mins
                ,total.hrs_played AS total_hrs
            FROM pods
            LEFT JOIN music ON pods.year = music.year
            LEFT JOIN total ON pods.year = total.year
        )
        SELECT
            year
            ,CONCAT(CAST(ROUND((music_hrs / total_hrs)*100,2) AS VARCHAR),'%') AS music_pct
            ,CONCAT(CAST(ROUND((pod_hrs / total_hrs)*100,2) AS VARCHAR),'%') AS pod_pct
        FROM joined
        ORDER BY year
    '''
)
# %%
