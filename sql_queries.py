import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST=config.get('CLUSTER','HOST')
DB_NAME=config.get('CLUSTER','DB_NAME')
DB_USER=config.get('CLUSTER','DB_USER')
DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
DB_PORT=config.get('CLUSTER','DB_PORT')

ARN=config.get('IAM_ROLE','ARN')

LOG_DATA=config.get('S3','LOG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
SONG_DATA=config.get('S3','SONG_DATA')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar, 
    item_in_session int,
    last_name varchar,
    length float,
    level varchar, 
    location varchar,
    method varchar,
    page varchar,
    registration float, 
    session_id varchar,
    song varchar,
    status varchar,
    ts bigint,
    user_agent varchar,
    user_id varchar);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int,
    artist_id varchar,
    artist_latitude float, 
    artist_longitude float,
    artist_location varchar,
    artist_name varchar, 
    song_id varchar,
    title varchar,
    duration float,
    year smallint);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint IDENTITY(0,1),
    start_time timestamp NOT NULL sortkey, 
    user_id varchar NOT NULL,
    level varchar,
    song_id text NOT NULL distkey, 
    artist_id text NOT NULL,
    session_id varchar NOT NULL,
    location varchar, 
    user_agent varchar,
    primary key(songplay_id),
    foreign key(start_time) references time(start_time), 
    foreign key(user_id) references users(user_id),
    foreign key(song_id) references songs(song_id), 
    foreign key(artist_id) references artists(artist_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id varchar sortkey,
    first_name varchar,
    last_name varchar, 
    gender varchar(1),
    level varchar,
    primary key(user_id))
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar sortkey distkey,
    title varchar,
    artist_id varchar NOT NULL, 
    year smallint,
    duration float,
    primary key(song_id), 
    foreign key(artist_id) references artists(artist_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar sortkey,
    name varchar,
    location varchar,
    latitude float, 
    longitude float,
    primary key(artist_id))
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp sortkey,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint,
    primary key(start_time)) 
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy {} from {}
credentials 'aws_iam_role={}'
json {}
region 'us-west-2';
""").format("staging_events",LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""copy {} from {}
credentials 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format("staging_songs",SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, 
user_agent)
SELECT DISTINCT ts/1000 as start_time, user_id, level, song_id, artist_id, 
session_id, location, user_agent
FROM staging_events
JOIN staging_songs
ON staging_events.song = staging_songs.title 
AND staging_events.artist = staging_songs.artist_name
WHERE page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, 
artist_longitude
FROM staging_songs
""")

# Here we need to pay attention: the original time is an epoch and not a 
# timestamp
time_table_insert = ("""
INSERT INTO time 
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT se.start_time/1000 as start_time, 
EXTRACT(hour FROM start_time) as hour, 
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week, 
EXTRACT(month FROM start_time) as month, 
EXTRACT(year FROM start_time) as year,
EXTRACT(weekday FROM start_time) as weekday
FROM staging_events AS se
WHERE page='NextSong'

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
user_table_create, artist_table_create, time_table_create, song_table_create, 
songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, 
songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, 
time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, 
song_table_insert, artist_table_insert, time_table_insert]

