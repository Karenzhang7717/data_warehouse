import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE staging_events
                            (songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
                            artist VARCHAR,
                            auth VARCHAR,
                            first_name VARCHAR,
                            gender VARCHAR,
                            item_in_session INTEGER,
                            last_name VARCHAR,
                            length FLOAT,
                            level VARCHAR,
                            location VARCHAR,
                            method VARCHAR,
                            page VARCHAR,
                            registration FLOAT,
                            session_id INTEGER,
                            song VARCHAR,
                            status INTEGER,
                            ts BIGINT,
                            user_agent VARCHAR,
                            user_id INTEGER);
                            
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs
                        ( num_songs INTEGER,
                         artist_id VARCHAR(25) NOT NULL,
                         artist_latitude FLOAT,
                         artist_longitude FLOAT,
                         artist_name VARCHAR,
                         song_id VARCHAR(25) PRIMARY KEY NOT NULL,
                         title VARCHAR,
                         duration FLOAT,
                         year INT);
                         
""")

songplay_table_create = (""" CREATE TABLE songplay
                        (songplay_id INTEGER PRIMARY KEY NOT NULL,
                        start_time TIMESTAMP,
                        user_id INTEGER,
                        level VARCHAR(25),
                        song_id VARCHAR(25),
                        artist_id VARCHAR(25) sortkey distkey,
                        session_id INTEGER,
                        location VARCHAR(50),
                        user_agent VARCHAR(50)
                        );
""")

user_table_create = (""" CREATE TABLE users
                        (user_id INTEGER PRIMARY KEY NOT NULL sortkey,
                        first_name VARCHAR(25),
                        last_name VARCHAR(25),
                        gender VARCHAR(5),
                        level VARCHAR(25)
                        ) diststyle all;
""")

song_table_create = (""" CREATE TABLE songs
                        (song_id VARCHAR(25) PRIMARY KEY NOT NULL sortkey,
                         title VARCHAR,
                         artist_id VARCHAR(25) NOT NULL distkey,
                         year INT,
                         duration FLOAT
                         );
""")

artist_table_create = (""" CREATE TABLE artists
                        (artist_id VARCHAR(25) PRIMARY KEY NOT NULL sortkey distkey,
                        artist_name VARCHAR,
                        location VARCHAR(50),
                         artist_latitude FLOAT,
                         artist_longitude FLOAT
                        );
""")

time_table_create = (""" CREATE TABLE time
                    (start_time TIMESTAMP PRIMARY KEY sortkey,
                     hour INTEGER,
                     day INTEGER,
                     week INTEGER,
                     month INTEGER,
                     year INTEGER,
                     weekday VARCHAR(25)
                    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from 's3://udacity-dend/{}' 
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format('staging_events','log_json_path.json',config.get("IAM_ROLE","ARN"))

staging_songs_copy = ("""
copy {} from 's3://udacity-dend/{}' 
credentials 'aws_iam_role={}'
format as json 'auto' region 'us-west-2';
""").format('staging_songs','song_data',config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (songplay_id, start_time, user_id, level,song_id,
                      artist_id, session_id, location, user_agent)
SELECT se.songplay_id,
       to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') as start_time,
       se.user_id as user_id,
       se.level as level,
       ss.song_id as song_id,
       ss.artist_id as artist_id,
       se.session_id as session_id,
       se.location as location,
       se.user_agent as user_agent
FROM staging_events se JOIN staging_songs ss ON se.song=ss.title AND se.artist=ss.artist_name
WHERE se.page='NextSong' AND se.ts IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level
FROM staging_events
WHERE page = 'NextSong' AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
               
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, location, artist_latitude, artist_longitude)
SELECT DISTINCT artist_id, 
                artist_name, 
                location, 
                artist_latitude, 
                artist_longitude
FROM artists
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, 
                EXTRACT(hour from start_time),
                EXTRACT(day from start_time),
                EXTRACT(week from start_time),
                EXTRACT(month from start_time),
                EXTRACT(year from start_time),
                EXTRACT(weekday from start_time)
FROM staging_events WHERE ts IS NOT NULL;
""")

# QUERY LISTS
# create_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
