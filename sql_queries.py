# DROP TABLES
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists  users"
song_table_drop = "drop table if exists  songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table  if exists time_table"

# CREATE TABLES
songplay_table_create = ("create table if not exists songplay( \
                        ts varchar, \
                        userId varchar, \
                        level varchar, \
                        songId varchar, \
                        artistId varchar, \
                        sessionId varchar, \
                        location varchar, \
                        userAgent varchar \
                        )")

user_table_create = ("create table if not exists users( \
                        userId varchar, \
                        firstName varchar, \
                        lastName varchar, \
                        gender varchar, \
                        level varchar \
                        )")

song_table_create = ("create table if not exists songs( \
                        song_id varchar primary key, \
                        title varchar, \
                        year int, \
                        duration float \
                        )")

artist_table_create = ("create table if not exists artists(artist_id varchar, name varchar, location varchar, latitude float, longitude float, song_id varchar)")

time_table_create = ("create table if not exists time_table(timestamp timestamp, hour int, day int, week_of_year int,month int, year int, week_of_day int)")

# INSERT RECORDS

songplay_table_insert = ("insert into songplay(ts, userId, level, songId, artistId, sessionId, location, userAgent) values(%s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ("insert into users(userId, firstName, lastName, gender, level) values(%s, %s, %s, %s, %s)")

song_table_insert = ("insert into songs(song_id, title, year, duration) values(%s, %s, %s, %s)")

artist_table_insert = ("insert into artists(artist_id, name, location, latitude, longitude, song_id) values(%s, %s, %s, %s, %s, %s)")


time_table_insert = ("insert into time_table(timestamp, hour, day, week_of_year, month, year, week_of_day) values(%s, %s, %s, %s, %s, %s, %s)")

# FIND SONGS

song_select = ("select artists.artist_id, songs.song_id \
                from songs \
                join artists \
                on songs.song_id = artists.song_id where songs.title = '{SONG_NAME}' \
                and artists.name = '{ARTIST_NAME}' \
                and songs.duration = {LENGTH}")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]