import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('config/dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ('''
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length REAL,
        level VARCHAR (10),
        location VARCHAR,
        method CHAR(3),
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INT);
''')

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int,
        artist_id text,
        artist_name varchar(max),
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location text,
        song_id text,
        title text,
        duration numeric,
        year int
    )
""")

songplay_table_create = ('''
    CREATE TABLE IF NOT EXISTS songplay(
        songplay_id INT IDENTITY (0, 1) PRIMARY KEY,
        start_time BIGINT NOT NULL REFERENCES time sortkey,
        user_id INT NOT NULL REFERENCES users,
        level VARCHAR,
        song_id VARCHAR REFERENCES songs distkey,
        artist_id VARCHAR REFERENCES artists,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR);
''')

user_table_create = ('''
    CREATE TABLE IF NOT EXISTS users(
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR (10)
    ) DISTSTYLE all;
''')

song_table_create = ('''
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY distkey sortkey,
        artist_id VARCHAR NOT NULL REFERENCES artists,
        year SMALLINT,
        duration REAL
    );
''')

artist_table_create = ('''
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude REAL,
        longitude REAL
    ) DISTSTYLE all;
''')

time_table_create = ('''
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    ) DISTSTYLE all;
''')

# STAGING TABLES

staging_events_copy = (''' COPY staging_events
                       FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       FORMAT AS JSON {}
                       REGION 'us-west-2'
                       TIME FORMAT AS 'epochmillisecs';
''').format(config['S3']['LOG_DATA'], config['CLUSTER']['DWH_ROLE_ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ('''COPY staging_songs
                       FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       REGION 'us-west-2'
                        FORMAT AS JSON 'auto';
''').format(config['S3']['SONG_DATA'], config['CLUSTER']['DWH_ROLE_ARN'])

# FINAL TABLES
# Use INSERT INTO SELECT

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender,
                    level
    FROM staging_events
    WHERE staging_events.userId IS NOT NULL
    AND staging_events.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songplay
    SELECT DISTINCT song_id,
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT start_time
                    EXTRACT (hour FROM start_time)
                    EXTRACT (day FROM start_time)
                    EXTRACT (week FROM start_time)
                    EXTRACT (month FROM start_time)
                    EXTRACT (dayofweek FROM start_time)
    FROM songplays;
""")

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, 
                            session_id, location, user_agent)
    SELECT DISTINCT 
    FROM staging_events ev
    JOIN staging_songs s ON (ev.song = s.title AND ev.artist = s.artist_name)
    AND ev.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, artist_table_create, user_table_create, 
                        song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
