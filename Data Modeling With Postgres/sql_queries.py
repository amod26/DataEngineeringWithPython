# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays
(songplay_id SERIAL PRIMARY KEY 
,start_time varchar 
, user_id varchar NOT NULL 
, level varchar
, song_id varchar 
, artist_id varchar
, session_id int 
, location varchar
, user_agent varchar);
""")

user_table_create = ("""create table if not exists users
(user_id varchar PRIMARY KEY , 
first_name varchar, 
last_name varchar,
gender varchar,
level varchar);
""")

song_table_create = ("""create table if not exists songs
(song_id varchar PRIMARY KEY , 
title varchar, 
artist_id varchar NOT NULL,
year int, 
duration float);
""")

artist_table_create = ("""create table if not exists artists
(artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar,
latitude float, 
longitude float);
""")

time_table_create = ("""create table if not exists time
(timestamp time, 
hour int,
day int, 
week int,
month int,
year int,
weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level,song_id,artist_id,session_id,location,user_agent) \
                 VALUES (%s, %s, %s, %s, %s,%s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users (user_id , first_name , last_name , gender , level ) \
                 VALUES (%s, %s, %s, %s, %s)
                 ON CONFLICT (user_id) 
                DO NOTHING;
""")

song_table_insert = ("""INSERT INTO songs(song_id , title , artist_id , year , duration ) \
                 VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id , name , location , latitude , longitude ) \
                 VALUES (%s, %s, %s, %s, %s)
                 ON CONFLICT (artist_id) 
                DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time(timestamp  , hour , day , week , month , year , weekday ) \
                 VALUES (%s, %s, %s, %s, %s,%s, %s)
""")

# FIND SONGS

song_select = ("""select s.song_id,a.artist_id
from songs s join artists a on s.artist_id=a.artist_id
where s.title=%s
and a.name=%s
and s.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]