# DROP TABLES

songplay_table_drop = "DROP TABLE songplays;"
user_table_drop = "DROP TABLE users;"
song_table_drop = "DROP TABLE songs;"
artist_table_drop = "DROP TABLE artists;"
time_table_drop = "DROP TABLE times;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays (
        songplay_id serial, 
        start_time timestamp, 
        user_id int, 
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar,
        PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""CREATE TABLE users (
        user_id int,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar,
        PRIMARY KEY (user_id)
);
""") 

song_table_create = ("""CREATE TABLE songs (
        song_id varchar, 
        title varchar,
        artist_id varchar,
        year int,
        duration numeric,
        PRIMARY KEY (song_id)
);
""") 

artist_table_create = ("""CREATE TABLE artists (
        artist_id varchar,
        name varchar,
        location varchar,
        latitude varchar,
        longitude varchar,
        PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""CREATE TABLE times (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
);
""")


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
""")

user_table_insert = ("""INSERT INTO users 
    (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
    DO NOTHING
""")

song_table_insert = ("""INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) 
    DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists 
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) 
    DO NOTHING
""")

time_table_insert = ("""INSERT INTO times 
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) 
    DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT song_id, artist_id
FROM songs
WHERE song_id = %s AND artist_id = %s AND duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# 