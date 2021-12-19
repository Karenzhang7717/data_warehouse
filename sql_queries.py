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
                            artist VARCHAR(50),
                            auth VARCHAR(25),
                            first_name VARCHAR(25),
                            gender VARCHAR(5),
                            item_in_session INTEGER,
                            last_name VARCHAR(25),
                            length FLOAT,
                            level VARCHAR(25),
                            location VARCHAR(50),
                            method VARCHAR(25),
                            page VARCHAR(25),
                            registration BIGINT,
                            session_id INTEGER,
                            song VARCHAR(50),
                            status INTEGER,
                            ts BIGINT,
                            user_agent VARCHAR(50),
                            user_id INTEGER);
                            
                         
                            
""")

staging_songs_table_create = (""" CREATE TABLE staging_songs
                        ( num_songs INTEGER,
                         artist_id VARCHAR(25) NOT NULL,
                         artist_latitude FLOAT,
                         artist_longitude FLOAT,
                         artist_name VARCHAR(25),
                         song_id VARCHAR(25) PRIMARY KEY NOT NULL,
                         title VARCHAR(50),
                         duration FLOAT,
                         year INT);
                         
""")

songplay_table_create = (""" CREATE TABLE songplay
                        (songplay_id INTEGER PRIMARY KEY NOT NULL,
                        start_time TIMESTAMP,
                        user_id INTEGER FOREIGN KEY REFERENCES users(user_id),
                        level VARCHAR(25) FOREIGN KEY REFERENCES users(level),
                        song_id VARCHAR(25) FOREIGN KEY REFERENCES songs(song_id),
                        artist_id VARCHAR(25)  FOREIGN KEY REFERENCES artists(artist_id) sortkey distkey,
                        session_id INTEGER,
                        location VARCHAR(50) FOREIGN KEY REFERENCES artist(location),
                        user_agent VARCHAR(50),
                        FOREIGN KEY start_time REFERENCES time(start_time),
                        
                        );
""")

user_table_create = (""" CREATE TABLE users
                        (user_id INTEGER PRIMARY KEY NOT NOT NULL sortkey,
                        first_name VARCHAR(25),
                        last_name VARCHAR(25),
                        gender VARCHAR(5),
                        level VARCHAR(25)
                        ) diststyle all;
""")

song_table_create = (""" CREATE TABLE songs
                        (song_id VARCHAR(25) PRIMARY KEY NOT NULL sortkey,
                         title VARCHAR(50),
                         artist_id VARCHAR(25) NOT NULL distkey,
                         year INT,
                         duration FLOAT
                         );
""")

artist_table_create = (""" CREATE TABLE artists
                        (artist_id VARCHAR(25) PRIMARY KEY NOT NULL sortkey distkey,
                        artist_name VARCHAR(25),
                        location VARCHAR(50),
                         artist_latitude FLOAT,
                         artist_longitude FLOAT
                        );
""")

time_table_create = (""" CREATE TABLE time
                    (start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey,
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
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS
# create_table_queries = [staging_events_table_create, staging_songs_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
