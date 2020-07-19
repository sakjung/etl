import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from typing import Iterator, Optional
import io
import json
from typing import Any
import datetime


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)
    

def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the times table.
    Then it extracts the user information in order to store it into the users table.
    Finally it extracts songplay information in order to store it into the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms').drop_duplicates(keep="last")
    
    # make file-like object (buffer iterator) to use copy_from function
    # enable bulk insert
    time_log = t.to_json(orient='values')
    
    time_string_iterator = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            datetime.datetime.fromtimestamp(timedata/1000.0),
            datetime.datetime.fromtimestamp(timedata/1000.0).hour,
            datetime.datetime.fromtimestamp(timedata/1000.0).day,
            datetime.datetime.fromtimestamp(timedata/1000.0).isocalendar()[1],
            datetime.datetime.fromtimestamp(timedata/1000.0).month,
            datetime.datetime.fromtimestamp(timedata/1000.0).year,
            datetime.datetime.fromtimestamp(timedata/1000.0).weekday(),
        ))) + '\n'
        for timedata in json.loads(time_log)
    ))
    
    cur.execute(create_time_temp_table)
    cur.copy_from(time_string_iterator, 'temp_t', sep='|')
    cur.execute(temp_to_time)
    
    # insert time data records
#     time_data = (t.tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.week.tolist(), t.dt.month.tolist(),t.dt.year.tolist(),t.dt.weekday.tolist())
#     column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
#     time_dict = {column_labels[i] : time_data[i] for i, _ in enumerate(column_labels)}
#     time_df = pd.DataFrame.from_dict(time_dict)

#     for i, row in time_df.iterrows():
#         cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.replace('', np.nan, inplace=True)
    user_df.dropna(subset=['userId'], inplace=True)
    user_df = user_df.drop_duplicates(subset='userId', keep="last")
    
    # make file-like object (buffer iterator) to use copy_from function
    # enable bulk insert
     
    user_log = user_df.to_json(orient='values')
    
    user_string_iterator = StringIteratorIO((
    '|'.join(map(clean_csv_value, (
        userdata[0],
        userdata[1],
        userdata[2],
        userdata[3],
        userdata[4],
    ))) + '\n'
    for userdata in json.loads(user_log)
    ))
    
    cur.execute(create_user_temp_table)
    cur.copy_from(user_string_iterator, 'temp_u', sep='|')
    cur.execute(temp_to_user)
    
#     # insert user records
#     for i, row in user_df.iterrows():
#         cur.execute(user_table_insert, row)
        
    # insert songplay records
    
    s = ""
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        if row['userId']=="":
            row['userId'] = None
    
        s += '|'.join(map(clean_csv_value, (
            pd.Timestamp(row[row.index.get_loc("ts")]), 
            row[row.index.get_loc("userId")],
            row[row.index.get_loc("level")],
            songid,  
            artistid,
            row[row.index.get_loc("sessionId")],
            row[row.index.get_loc("location")],  
            row[row.index.get_loc("userAgent")],
            ))) + '\n'
    
    songplay_string_iterator = StringIteratorIO(io.StringIO(s))
    cur.copy_from(songplay_string_iterator, 'songplays', sep='|', columns = ('start_time', 'user_id', 'level', 'song_id',\
                                                                             'artist_id', 'session_id', 'location', 'user_agent'))
    
#         # insert songplay record
#         songplay_data = (pd.Timestamp(row.ts), row.userId, row.level, songid,  artistid, row.sessionId, row.location, row.userAgent)
#         cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure figure out existing files in the given filepath which has been provided as an arugment.
    Then it applies func in the argument to the files figured out.

    INPUTS: 
    * cur the cursor variable
    * conn the connection to the database
    * filepath the file path to the song file
    * func the function (process) to be applied to the files 
    """
    
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
    """
    This procedure is the main process
    It comprises all the functions made above
    To build the data pipeline
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()