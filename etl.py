def process_song_file(cur, filepath):
    """
    Process_song_file is a function in charge of processing a single song file and loading needed data into our songs and artists
    table
    
    Args:
        cur: psycopg2 cursor to execute queries.
        filepath: one filepath with song_data.
    
    Return: 
        NONE
    
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # --- insert song record ---
    
    #Select desired columns for songs table...
    
    song_data = df.drop(['artist_latitude','artist_location','artist_longitude','artist_name','num_songs'],axis=1)
    
    #Reorganizing dataframe...
    
    song_data = df.reindex(columns= ['song_id', 'title', 'artist_id','year','duration'])
    
    #DF row to list and get first position of double bracket list...
    
    song_data = song_data.values.tolist()
    song_data = song_data[0]
    
    #Insert list in songs table...
    cur.execute(song_table_insert, song_data)
    
    
    
    # --- insert artist record ---
    
    #Select desired columns for artists table...
    artist_data = df.drop(['song_id', 'title','year','duration','num_songs'],axis=1)
    
    #Reorganizing dataframe...
    
    artist_data = artist_data.reindex(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'],axis=1)
    
    #DF row to list and get first position of double bracket list...
    
    artist_data = artist_data.values.tolist()
    artist_data = artist_data[0]
    
    #Insert list artists table...
    
    cur.execute(artist_table_insert, artist_data)
    
    
##Process_log_file function processes a single log_file and insert data into time, users and songplays tables...

def process_log_file(cur, filepath):
    """
    Process_log_file function processes a single log_file and insert data into time, users and songplays tables.
    
    Args:
        cur: psycopg2 cursor to execute queries.
        filepath: one filepath with log_data.
    
    Return: 
        NONE
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(arg= df['ts'], unit='ms')
    
    # insert time data records
    column_labels = ('timestamp','hour','day','week','month','year','day_of_week')

    time_df = pd.DataFrame(columns = column_labels)
    time_df['timestamp']=df['ts']
    time_df['hour']=pd.DatetimeIndex(df['ts']).hour
    time_df['day']=pd.DatetimeIndex(df['ts']).day
    time_df['week']= pd.DatetimeIndex(df['ts']).week
    time_df['month']=pd.DatetimeIndex(df['ts']).month
    time_df['year']=pd.DatetimeIndex(df['ts']).year
    time_df['day_of_week']= df['ts'].dt.dayofweek
    
    #Iterate over time_df to insert records

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """
    Process data will iterate over all our song or log files, process them with the functions above 
    and load them in the database in their tables.
    
    Args:
        cur: psycopg2 cursor to execute queries.
        conn: connection to sparkifydb database.
        filepath: log data and song data directory. 
        func: function in charge of processing one file and upload in db.
    
    Return: 
        NONE
    
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()