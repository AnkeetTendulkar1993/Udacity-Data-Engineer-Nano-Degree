import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT8,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       BIGINT,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs          INTEGER, 
    artist_id          VARCHAR, 
    artist_latitude    FLOAT8,
    artist_longitude   FLOAT8, 
    artist_location    VARCHAR,
    artist_name        VARCHAR, 
    song_id            VARCHAR, 
    title              VARCHAR,
    duration           FLOAT8, 
    year               INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id    BIGINT  IDENTITY(0,1)  NOT NULL  PRIMARY KEY, 
    start_time     TIMESTAMP  SORTKEY,
    user_id        BIGINT, 
    level          VARCHAR, 
    song_id        VARCHAR,
    artist_id      VARCHAR, 
    session_id     BIGINT, 
    location       VARCHAR, 
    user_agent     VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id       BIGINT  NOT NULL  PRIMARY KEY  SORTKEY, 
    first_name    VARCHAR, 
    last_name     VARCHAR, 
    gender        VARCHAR, 
    level         VARCHAR
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR  NOT NULL  PRIMARY KEY  SORTKEY, 
    title       VARCHAR, 
    artist_id   VARCHAR, 
    year        INTEGER, 
    duration    FLOAT8
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id     VARCHAR   NOT NULL  PRIMARY KEY  SORTKEY, 
    name          VARCHAR, 
    location      VARCHAR, 
    lattitude     FLOAT8, 
    longitude     FLOAT8
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time   TIMESTAMP  NOT NULL  PRIMARY KEY  SORTKEY, 
    hour         INTEGER    NOT NULL, 
    day          INTEGER    NOT NULL, 
    week         INTEGER    NOT NULL, 
    month        INTEGER    NOT NULL, 
    year         INTEGER    NOT NULL, 
    weekday      BOOLEAN    NOT NULL
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {source}
IAM_ROLE {iam_role}
JSON {json_log_path};
""").format(source=config['S3']['LOG_DATA'], iam_role=config['IAM_ROLE']['ARN'], json_log_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM {source}
IAM_ROLE {iam_role}
json 'auto';
""").format(source=config['S3']['SONG_DATA'], iam_role=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT TIMESTAMP 'epoch' + CAST(ts AS BIGINT)/1000 * INTERVAL '1 second' AS start_time, 
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
FROM staging_events se
LEFT JOIN staging_songs ss ON ( se.song = ss.title AND se.artist = ss.artist_name AND se.ts = ss.duration)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender AS gender,
        level AS level
FROM staging_events
WHERE page = 'NextSong' 
      AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(song_id) AS song_id,
        title, 
        artist_id, 
        year, 
        duration
FROM staging_songs 
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT  DISTINCT(artist_id) AS artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS lattitude, 
        artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT(start_time) AS start_time, 
        EXTRACT(hour FROM start_time) AS hour, 
        EXTRACT(day FROM start_time) AS day, 
        EXTRACT(week FROM start_time) AS week, 
        EXTRACT(month FROM start_time) AS month, 
        EXTRACT(year FROM start_time) AS year, 
        EXTRACT(dayofweek FROM start_time) AS weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]





