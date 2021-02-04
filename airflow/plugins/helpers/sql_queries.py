class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM {schema_name}.staging_events
            WHERE page='NextSong') events
            LEFT JOIN {schema_name}.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM {schema_name}.staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM {schema_name}.staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM {schema_name}.staging_songs
    """)

    time_table_insert = ("""
                        WITH time_table AS

                        (SELECT DISTINCT start_time

                        FROM 

                        {schema_name}.songplays
                        )
                        
                        select 
                        start_time,
                        EXTRACT (hour FROM start_time) AS hour,
                        EXTRACT (day FROM start_time) AS day,
                        EXTRACT (week FROM start_time) AS week,
                        EXTRACT (month FROM start_time) AS month,
                        EXTRACT (year FROM start_time) AS year,
                        EXTRACT (dayofweek FROM start_time) AS weekday
                        
                        FROM time_table""")