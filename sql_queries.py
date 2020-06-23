import configparser

# CONFIG MACROS
config = configparser.ConfigParser()
config.read('dwh.cfg')
ROLE = config.get('IAM_ROLE', 'ARN')
REGION = 'us-west-2'
PATH = config.get('S3', 'LOG_JSONPATH')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONGS_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES
staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS songs_staging \
(num_songs int, \
artist_id varchar(max), \
artist_latitude decimal, \
artist_longitude decimal, \
artist_location  varchar(max), \
artist_name varchar(max), \
song_id varchar(max), \
title  varchar(max), \
duration decimal, \
year int)
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS events_staging \
(artist varchar(max), \
auth varchar(max), \
firstName varchar(max), \
gender varchar(max), \
itemInSession int, \
lastName varchar(max), \
length decimal, \
level varchar(max), \
location varchar(max), \
method varchar(max), \
page varchar(max), \
registration bigint, \
sessionId int, \
song varchar(max), \
status int, \
ts bigint, \
userAgent varchar(max), \
userId int)
""")

# CREATE FINAL TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
(songplay_id bigint identity(0, 1), \
start_time timestamp NOT NULL REFERENCES time(start_time), \
user_id int NOT NULL REFERENCES users(user_id), \
level varchar(max) NOT NULL, \
song_id varchar(max) REFERENCES songs(song_id),
artist_id varchar(max) REFERENCES artists(artist_id), \
session_id int NOT NULL, \
location varchar(max), \
user_agent varchar(max), \
primary key(songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users \
(user_id int NOT NULL, \
first_name varchar(max), \
last_name varchar(max), \
gender varchar(max), \
level varchar(max), \
primary key(user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs \
(song_id varchar(max) NOT NULL, \
title varchar(500), \
artist_id varchar(max), \
year int, \
duration decimal, \
primary key(song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists \
(artist_id varchar(max) NOT NULL, \
name varchar(500), \
location varchar(500), \
latitude decimal, \
longitude decimal, \
primary key(artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time \
(start_time timestamp NOT NULL, \
hour int, \
day int, \
week int, \
month int, \
year int, \
weekday int, \
primary key(start_time))
""")

# COPY DATA FROM S3 TO STAGING TABLES
events_staging_copy = ("""
COPY events_staging FROM {} \
CREDENTIALS 'aws_iam_role={}' \
REGION '{}' \
FORMAT AS JSON {}
""").format(LOG_DATA, ROLE, REGION, PATH)

songs_staging_copy = ("""
COPY songs_staging FROM {} \
CREDENTIALS 'aws_iam_role={}' \
FORMAT AS JSON 'auto' \
REGION '{}'
""").format(SONGS_DATA, ROLE, REGION)

# POPULATE FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays \
(start_time, user_id, level, \
song_id, artist_id, session_id, \
location, user_agent) \
    SELECT \
        TIMESTAMP 'epoch' + events.ts/1000 * 
        INTERVAL '1 second' AS start_time, events.userId, \
        events.level, songs.song_id, \
        songs.artist_id, events.sessionId, \
        events.location, events.userAgent \
    FROM \
        events_staging events \
        JOIN \
        songs_staging songs \
        ON  events.song = songs.title \
    WHERE \
        events.page = 'NextSong'
""")


user_table_insert = ("""
INSERT INTO users \
(user_id, first_name, \
last_name, gender, level) \
    SELECT \
        DISTINCT(events.userId), events.firstName, \
        events.lastName, events.gender, events.level \
    FROM \
        events_staging events\
    WHERE \
        events.page = 'NextSong' \
        AND events.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs \
(song_id, title, \
artist_id, year, duration) \
    SELECT \
        DISTINCT(songs.song_id), songs.title, \
        songs.artist_id, songs.year, songs.duration \
    FROM \
        songs_staging songs \
    WHERE \
        songs.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists \
(artist_id, name, \
location, latitude, longitude) \
    SELECT \
        DISTINCT(songs.artist_id), songs.artist_name, \
        songs.artist_location, songs.artist_latitude, \
        songs.artist_longitude \
    FROM \
        songs_staging songs \
    WHERE \
        songs.song_id IS NOT NULL
""")


time_table_insert = ("""
INSERT INTO time \
(start_time, hour, day, \
week, month, year, weekday) \
    SELECT \
        DISTINCT TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time, \
        EXTRACT(HOUR FROM start_time), \
        EXTRACT(DAY FROM start_time), \
        EXTRACT(WEEK FROM start_time), \
        EXTRACT(MONTH FROM start_time), \
        EXTRACT(YEAR FROM start_time), \
        EXTRACT(DOW FROM start_time) \
    FROM \
        events_staging events \
    WHERE \
        events.page = 'NextSong' \
        AND events.ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        time_table_create, user_table_create, song_table_create,
                        artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [events_staging_copy, songs_staging_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]
