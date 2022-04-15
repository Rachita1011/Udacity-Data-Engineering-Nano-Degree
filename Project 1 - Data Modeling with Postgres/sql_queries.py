# DROP TABLES
# CREATING QUERIES TO DROP THE TABLES IF THEY ALREADY EXIST
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES
# NEXT QUERIES WILL CREATE THE TABLES 
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS SONGPLAYS ( SONGPLAY_ID SERIAL PRIMARY KEY, 
                                                                    START_TIME TIMESTAMP, 
                                                                    USER_ID INT, 
                                                                    LEVEL VARCHAR NOT NULL, 
                                                                    SONG_ID VARCHAR, 
                                                                    ARTIST_ID VARCHAR, 
                                                                    SESSION_ID INT NOT NULL, 
                                                                    LOCATION VARCHAR NOT NULL, 
                                                                    USER_AGENT VARCHAR NOT NULL)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS USERS ( USER_ID VARCHAR PRIMARY KEY, 
                                                           FIRST_NAME VARCHAR, 
                                                           LAST_NAME VARCHAR,
                                                           GENDER VARCHAR(1), 
                                                           LEVEL VARCHAR NOT NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS SONGS ( SONG_ID VARCHAR PRIMARY KEY, 
                                                           TITLE VARCHAR, 
                                                           ARTIST_ID VARCHAR, 
                                                           YEAR INT, 
                                                           DURATION FLOAT)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS ARTISTS ( ARTIST_ID VARCHAR PRIMARY KEY, 
                                                               NAME VARCHAR, 
                                                               LOCATION VARCHAR, 
                                                               LATITUDE DECIMAL, 
                                                               LONGITUDE DECIMAL)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS TIME ( START_TIME TIMESTAMP PRIMARY KEY, 
                                                          HOUR INT, 
                                                          DAY INT, 
                                                          WEEK INT, 
                                                          MONTH INT, 
                                                          YEAR INT, 
                                                          WEEKDAY VARCHAR)""")

# INSERT RECORDS
# Queries to insert data into tables
songplay_table_insert = (""" INSERT INTO SONGPLAYS ( START_TIME, 
                                                      USER_ID, 
                                                      LEVEL, 
                                                      SONG_ID, 
                                                      ARTIST_ID,  
                                                      SESSION_ID, 
                                                      LOCATION, 
                                                      USER_AGENT) 
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT DO NOTHING;""")

user_table_insert = (""" INSERT INTO USERS ( USER_ID, 
                                             FIRST_NAME, 
                                             LAST_NAME, 
                                             GENDER, 
                                             LEVEL) 
                        VALUES(%s, %s, %s, %s, %s) 
                        ON CONFLICT (USER_ID) DO UPDATE
                        SET level = EXCLUDED.level;""")

song_table_insert = (""" INSERT INTO SONGS ( SONG_ID, 
                                             TITLE, 
                                             ARTIST_ID, 
                                             YEAR, 
                                             DURATION) 
                        VALUES(%s, %s, %s, %s, %s) 
                        ON CONFLICT DO NOTHING;""")

artist_table_insert = (""" INSERT INTO ARTISTS ( ARTIST_ID, 
                                                 NAME, 
                                                 LOCATION, 
                                                 LATITUDE, 
                                                 LONGITUDE) 
                            VALUES(%s, %s, %s, %s, %s) 
                            ON CONFLICT DO NOTHING;""")


time_table_insert = (""" INSERT INTO TIME ( START_TIME, 
                                            HOUR, 
                                            DAY, 
                                            WEEK, 
                                            MONTH, 
                                            YEAR, 
                                            WEEKDAY) 
                        VALUES(%s, %s, %s, %s, %s, %s, %s) 
                        ON CONFLICT DO NOTHING;""")

# FIND SONGS
# Query written based on requirement mentioned in etl.ipynb file
song_select = (""" SELECT SONGS.SONG_ID, ARTISTS.ARTIST_ID 
                   FROM SONGS JOIN ARTISTS ON SONGS.ARTIST_ID = ARTISTS.ARTIST_ID 
                   WHERE SONGS.TITLE = %s 
                   AND ARTISTS.NAME = %s
                   AND SONGS.DURATION = %s ;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]