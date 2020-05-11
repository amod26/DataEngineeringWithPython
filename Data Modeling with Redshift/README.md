# Data Warehousing in Redshift <br>

- A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. <br>

- We will be  building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Here we will test the database and ETL pipeline by running queries given by the analytics team from Sparkify. <br>


## Schema Design <br>

- Fact table  <br>
    - songplays: log data associated with song plays songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent <br>

- Dimentions table <br>
    - users: users in the app user_id, first_name, last_name, gender, level <br>

    - songs: songs in music database song_id, title, artist_id, year, duration <br>

    - artists: artists in music database artist_id, name, location, latitude, longitude <br>

    - time: timestamps of records in songplays start_time, hour, day, week, month, year, weekday  <br>

## Staging table  <br>

- _staging_events_ dumping event log files from S3 directly in this table _stage_songs_ dumping songs files from S3 directly in this table. <br>

## Scripts <br>
- dwh.cfg (contains the details to connect to the redshift cluster) <br>
- sql_queries.py (contains CRUD statements)
- create_tables.py (contains the functions to import from sql_queries.py for creating tables) <br>
- etl.py (contains the import function from sql_queries.py for ETL) <br>
