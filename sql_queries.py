import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                 artist varchar(max),
                                 auth varchar(max),
                                 first_name varchar(max),
                                 gender char,
                                 item int,
                                 last_name varchar(max),
                                 length decimal,
                                 level varchar(max),
                                 location varchar(max),
                                 method varchar(max),
                                 page varchar(max),
                                 registration bigint,
                                 session_id int,
                                 song varchar(max),
                                 status int,
                                 ts bigint,
                                 user_agent varchar(max),
                                 user_id int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                 num_songs int,
                                 artist_id varchar(max),
                                 artist_latitude decimal,
                                 artist_longitude decimal,
                                 artist_location varchar(max),
                                 artist_name varchar(max),
                                 song_id varchar(max),
                                 title varchar(max),
                                 duration decimal,
                                 year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                            start_time timestamp REFERENCES time(start_time),
                            user_id int NOT NULL REFERENCES users(user_id),
                            level varchar(max) NOT NULL,
                            song_id varchar(max) REFERENCES songs(song_id),
                            artist_id varchar(max) REFERENCES artists(artist_id),
                            session_id int NOT NULL,
                            location varchar(max),
                            user_agent varchar(max) NOT NULL)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (user_id int PRIMARY KEY,
                        first_name varchar(max) NOT NULL,
                        last_name varchar(max),
                        gender char,
                        level varchar(max) NOT NULL)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (song_id varchar(max) PRIMARY KEY,
                        title varchar(max) NOT NULL,
                        artist_id varchar(max),
                        year int,
                        duration decimal)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                          (artist_id varchar(max) PRIMARY KEY,
                          name varchar(max) NOT NULL,
                          location varchar(max),
                          latitude decimal,
                          longitude decimal)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (start_time timestamp PRIMARY KEY,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON {}
                          REGION 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         FORMAT AS JSON 'auto'
                         REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,
                                                   user_id,
                                                   level,
                                                   song_id,
                                                   artist_id,
                                                   session_id,
                                                   location,
                                                   user_agent)
                            SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
                                   e.user_id,
                                   e.level,
                                   s.song_id,
                                   s.artist_id,
                                   e.session_id,
                                   e.location,
                                   e.user_agent
                            FROM staging_events e
                            JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
                            WHERE e.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id,
                                           first_name,
                                           last_name,
                                           gender,
                                           level)
                        SELECT DISTINCT user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                        FROM staging_events
                        WHERE page = 'NextSong'
                        AND user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs(song_id,
                                          title,
                                          artist_id,
                                          year,
                                          duration)
                        SELECT DISTINCT song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,
                                               name,
                                               location,
                                               latitude,
                                               longitude)
                          SELECT DISTINCT artist_id,
                                          artist_name,
                                          artist_location,
                                          artist_latitude,
                                          artist_longitude
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO time (start_time,
                                          hour,
                                          day,
                                          week,
                                          month,
                                          year,
                                          weekday)
                        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                                        EXTRACT(hour FROM start_time),
                                        EXTRACT(day FROM start_time),
                                        EXTRACT(week FROM start_time),
                                        EXTRACT(month FROM start_time),
                                        EXTRACT(year FROM start_time),
                                        EXTRACT(dow FROM start_time)
                        FROM staging_events
                        WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
