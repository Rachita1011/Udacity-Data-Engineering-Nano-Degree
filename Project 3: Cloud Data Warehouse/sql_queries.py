import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
REGION=config.get("AWS", "REGION_NAME")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = " DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INT
); 
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP  REFERENCES time(start_time)    sortkey,
        user_id INT           REFERENCES users(user_id)      distkey,
        level VARCHAR,
        song_id VARCHAR       REFERENCES songs (song_id),
        artist_id VARCHAR     REFERENCES artist(artist_id),
        session_id INT        NOT NULL,
        location VARCHAR,
        user_agent VARCHAR,
        PRIMARY KEY (songplay_id)
);                     
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
        user_id INT          distkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR,
        PRIMARY KEY (user_id)
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR      sortkey,
        title VARCHAR        NOT NULL,
        artist_id VARCHAR    NOT NULL,
        year INT,
        duration FLOAT,
        PRIMARY KEY (song_id)
);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR    sortkey,
        name VARCHAR         NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT,
        PRIMARY KEY (artist_id)
);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP  sortkey,
        hour INT              NOT NULL,
        day INT               NOT NULL,
        week INT              NOT NULL,
        month INT             NOT NULL,
        year INT              NOT NULL,
        weekday INT           NOT NULL,
        PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
        FROM {0}
        IAM_ROLE {1}
        REGION {2}
        JSON {3}
;
""").format(LOG_DATA, ARN, REGION, LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs
        FROM {0}
        IAM_ROLE {1}
        REGION {2}
        ;
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT     se.ts, 
                        se.userId, 
                        se.level, 
                        ss.song_id, 
                        ss.artist_id, 
                        se.sessionId, 
                        se.location, 
                        se.userAgent
    FROM staging_events as se
    INNER JOIN staging_songs as ss
    ON se.song = ss.title and se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
    
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT     se.userId,
                        se.firstName,
                        se.lastName,
                        se.gender,
                        se.level
    FROM staging_events as se
    WHERE se.user_Id IS NOT NULL;
    
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT     ss.song_id,
                        ss.title,
                        ss.artist_id,
                        ss.year,
                        ss.duration
    FROM staging_songs as ss
    WHERE ss.song_id IS NOT NULL;
    
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT     ss.artist_id,
                        ss.artist_name,
                        ss.artist_location,
                        ss.artist_latitude,
                        ss.artist_longitude
    FROM staging_songs as ss
    WHERE ss.artist_id IS NOT NULL;
                        
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT     se.ts,
                        EXTRACT(hour from se.ts),
                        EXTRACT(day from se.ts),
                        EXTRACT(week from se.ts),
                        EXTRACT(month from se.ts),
                        EXTRACT(year from se.ts),
                        EXTRACT(weekday from se.ts)
    FROM staging_events as se
    WHERE se.page = 'NextSong';
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
