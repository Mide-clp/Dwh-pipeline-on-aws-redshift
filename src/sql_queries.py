from dotenv import load_dotenv
import os

load_dotenv()

DWH_ROLE_ARN = os.getenv("DWH_ROLE_ARN")
REGION = os.getenv("REGION")

s3_log_data = os.getenv("LOG_DATA")
s3_song_data = os.getenv("SONG_DATA")
log_json_path = os.getenv("LOG_JSON_PATH")

##### DROP TABLES #######

songplays_drop = "DROP TABLE IF EXISTS songplays"
users_drop = "DROP TABLE IF EXISTS users"
artists_drop = "DROP TABLE IF EXISTS artists"
songs_drop = "DROP TABLE IF EXISTS songs"
time_drop = "DROP TABLE IF EXISTS time"
staging_event_drop = "DROP TABLE IF EXISTS staging_events"
staging_song_drop = "DROP TABLE IF EXISTS staging_songs"

##### CREATE TABLES #######

staging_event_create = \
    """
     CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar, auth varchar,
        firstName varchar, gender char(1),
        itemInSession smallint,lastName varchar,
        length varchar, level varchar,
        location varchar, method varchar,
        page varchar, registration numeric,
        sessionId smallint, song text,
        status smallint, ts bigint,
        userAgent text, userId int
    );
    """

staging_song_create = \
    """ 
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs smallint, artist_id varchar,
        artist_latitude float, artist_longitude float,
        artist_location text, artist_name varchar,
        song_id varchar, title text,
        duration float, year smallint
    );
    """

songplays_create = \
    """ 
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1), start_time TIMESTAMP, 
        user_id int, level varchar, 
        song_id varchar, artist_id varchar,
        session_id int, location text, 
        user_agent text,
        PRIMARY KEY (songplay_id),
        FOREIGN KEY (start_time) REFERENCES time(start_time),
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    );
    """

users_create = \
    """
     CREATE TABLE IF NOT EXISTS users (
         user_id int, first_name varchar, 
         last_name varchar, gender varchar, 
         level varchar, 
         PRIMARY KEY(user_id)
     );
    """

songs_create = \
    """
     CREATE TABLE IF NOT EXISTS songs (
         song_id varchar, title text, 
         artist_id varchar, year smallint, 
         duration float, 
         PRIMARY KEY (song_id),
         FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
     );
    """

artists_create = \
    """
     CREATE TABLE IF NOT EXISTS artists (
         artist_id text, name varchar, 
         location text, latitude float, 
         longitude float,
         PRIMARY KEY(artist_id)
     );
    """

time_create = \
    """
     CREATE TABLE IF NOT EXISTS time (
         start_time TIMESTAMP, hour smallint, 
         day int, week smallint, 
         month int, year smallint, 
         weekday smallint, 
         PRIMARY KEY(start_time)
     );
    """

##### COPY TABLES #######

copy_staging_event = \
    f"""
    copy staging_events
    from '{s3_log_data}'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    region '{REGION}'
    json'{log_json_path}'
    """


copy_staging_song = \
    f"""
    copy staging_songs
    from '{s3_song_data}'
    credentials 'aws_iam_role={DWH_ROLE_ARN}'
    region '{REGION}'
    json 'auto'
    """

##### INSERT TABLES #######

artist_insert = \
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    """

song_insert = \
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    """

time_insert = \
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
    timestamp with time zone 'epoch' + ts/1000 * interval '1 second' as start_time,
    extract (hour from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as hour,
    extract (day from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as day,
    extract (week from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as week,
    extract (month from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as month,
    extract (year from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as year,
    extract (dow from timestamp with time zone 'epoch' + ts/1000 * interval '1 second') as weekday
    FROM staging_events
    """

user_insert = \
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
    """

songplays_insert = \
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
    timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
    FROM staging_events as se
    INNER JOIN staging_songs as ss on
    se.song = ss.title AND
    se.artist = ss.artist_name AND
    se.length = ss.duration
    WHERE se.page = 'NextSong'
    """

#### SELECT STATEMENT #####

songplay_select = "SELECT COUNT(*) FROM songplays;"

user_select = "SELECT COUNT(*) FROM users;"

songs_select = "SELECT COUNT(*) FROM songs;"

artists_select = "SELECT COUNT(*) FROM artists;"

time_select = "SELECT COUNT(*) FROM time;"


#### QUERY LIST #####
drop_tables_statement = [staging_event_drop, staging_song_drop, songplays_drop, time_drop, songs_drop, artists_drop,
                         users_drop]
create_tables_statement = [staging_event_create, staging_song_create, time_create, artists_create, songs_create,
                           users_create, songplays_create]
copy_table_statement = [copy_staging_event, copy_staging_song]
insert_table_statement = [artist_insert, song_insert, time_insert, user_insert, songplays_insert]
select_statement = [songplay_select, user_select, songs_select, artists_select, time_select]

