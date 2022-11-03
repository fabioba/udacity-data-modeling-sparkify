import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

import logging

logger = logging.getLogger(__name__)

def process_song_file(cur, filepath):
    try:
        # open song file
        df = pd.read_json(filepath, lines=True)

        # insert song record
        song_data = list(df[['song_id','title','year','duration']].loc[0,:])

        song_id = song_data[0]
        title = song_data[1]
        year = song_data[2].item()
        duration = song_data[3].item()

        song_data = [song_id, title, year, duration]

        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude','song_id']].iloc[0,:])

        artist_id = artist_data[0]
        artist_name = artist_data[1]
        artist_location = artist_data[2]
        artist_latitude = artist_data[3]
        artist_longitude = artist_data[4]
        song_id = artist_data[5]

        song_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude, song_id]
        
        
        cur.execute(artist_table_insert, artist_data)
        
    except Exception as err:
        logger.exception(err)
        raise err


def process_log_file(cur, filepath):
    try:
        # open log file
        df = pd.read_json(filepath, lines=True)

        # filter by NextSong action
        df = df[df['page']=='NextSong']

        # convert timestamp column to datetime
        t = pd.to_datetime(df['ts'], unit='ms')

        # insert time data records
        time_data = t
        column_labels = {
                'timestamp':time_data.values,
                'hour':time_data.dt.hour,
                'day':time_data.dt.day,
                'week_of_year':time_data.dt.weekofyear,
                'month':time_data.dt.month,
                'year':time_data.dt.year,
                'week_of_day':time_data.dt.weekday
            } 
        time_df = pd.DataFrame(data=column_labels)

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        # load user table
        user_df = df[['userId','firstName','lastName','gender','level']]

        # insert user records
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

        # insert songplay records
        for index, row in df.iterrows():
            print('index:{}'.format(index))
            # get songid and artistid from song and artist tables
            cur.execute(song_select.format(SONG_NAME=row.song.replace("'","''") , ARTIST_NAME = row.artist.replace("'","''"), LENGTH = row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            df['songid'] = songid
            df['artistid'] = artistid


            # insert songplay record
            songplay_data = list(df[['ts', 'userId', 'level', 'songid', 'artistid', 'sessionId', 'location', 'userAgent']])
            cur.execute(songplay_table_insert, songplay_data)
            
    except Exception as err:
        logger.exception(err)
        raise err


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()