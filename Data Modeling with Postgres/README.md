Data Modeling for Music Streaming App

Purpose

The startup called Sparkify wants to analyze the data collected about the user activity on a newly developed new music streaming app. The purpose of this project is to build a Data Model that would provide a easy way for analysis of this data in order to understand user behaviour for further enhancing the product and creating a better and more personalized experience.

Data Model

Fact Table

SongPlays
This table provides records with details of the songs played by the user.
Columns - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

Users
These are the users in the music app.
Columns - user_id, first_name, last_name, gender, level

Songs
This provides data about each song in the music database
Columns - song_id, title, artist_id, year, duration

Artists
This provides data each of the artists in music database
Columns - artist_id, name, location, latitude, longitude

Time
This table has timestamps of records in songplays broken down into specific units
Columns - start_time, hour, day, week, month, year, weekday

Files

Input Data

The data folder consists of data used to populate the Dimension tables and Fact Table.

There are two datasets available as mentioned below.

[1] Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

This data is used to populate the Songs and Artists table.

[2] Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

The SongPlays, Users and Time tables are created with from data.


create_tables.py

This script consists of a python wrapper code used to drop existing tables and create new tables in the database.

etl.ipynb

This is a notebook used for the purpose of development of the ETL code.

etl.py

This is a python script used to read and process the input data and load it into the Fact and Dimension tables.

sql_queries.py

This script consists of all the SQL queries for creation of tables and transforming and loading data into the Fact and Dimension tables.

test.ipynb

This is a notebook used to check the data inserted into the Fact and Dimension tables.


Execution

Open the terminal.

Execute the create_tables.py script to create the Fact and Dimension Tables.

root@35434aabf140:/home/workspace# python create_tables.py

Execute the etl.py script to extract, transform and load the data into the Fact and Dimension Tables.

root@35434aabf140:/home/workspace# python etl.py 


Conclusion

Using this Star Schema queries can be written to for the purpose of analytics.

For example:

[1] Number of paid and free subscribers who streamed music in the month of August 2020.

SELECT u.level, COUNT(DISTINCT user_id)
FROM songplays s
LEFT JOIN users u ON s.user_id = u.user_id
INNER JOIN time t s.start_time = t.start_time
WHERE t.year = 2020 AND t.month = 8 
GROUP BY u.level
