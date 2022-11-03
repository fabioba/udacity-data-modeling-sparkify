# UDACITY-DATA-MODELING-SPARKIFY



## Purpose
The purpose of this database is encapsulate all the information related to the events streaming on the platform.

## Getting Started
To run the ETL pipeline start enter in `main` file and there is the place where the magic happens!
More in details:
- create tables, from create_table.py
- populate tables, from etl.py

## Schema
Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level
songs - songs in music database
song_id, title, artist_id, year, duration
artists - artists in music database
artist_id, name, location, latitude, longitude
time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday