<h1>Readme<h1> <br>
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. <br> <br>

We created a Postgres database schema and ETL pipeline for this analysis with tables designed to optimize queries on song play analysis.

 <h2>Schema Design</h2> <br>
    <h3>Fact table</h3> <br>
songplays - Records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
 
    <h3>Dimentions table</h3> <br>
users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

 
    <h3>Scripts</h3> <br>
   
sql_queries.py - includes (CRUD) queries <br>
create_tables.py - Resets the database by creating and dropping tables. <br>
etl.py - loads the json files for songs and logs dataset from the data folder on the db in star schema structure <br>

<h3>Steps to Run:</h3> <br>

1. python create_tables.py <br>
2. python etl.py