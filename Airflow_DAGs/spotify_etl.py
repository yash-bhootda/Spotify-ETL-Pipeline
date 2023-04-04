import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
from io import StringIO

from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util


def spotify_etl_func():

    spotify_client_id = "b7ed80b8ba7e4e0297457b4063c3a375"
    spotify_client_secret = "53bc721903df42caad1964b87821ddd2"

    spotify_redirect_url = "http://localhost:8080"

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotify_client_id,
                                                   client_secret=spotify_client_secret,
                                                   redirect_uri=spotify_redirect_url,
                                                   scope="user-read-recently-played"))
    recently_played = sp.current_user_recently_played(limit=50)
    # df = pd.read_csv(StringIO(str(recently_played)))
    # df = pd.DataFrame.from_dict(recently_played)
    # df.to_csv("yrb.csv")

    # if the length of recently_played is 0 for some reason just exit the program
    if len(recently_played) == 0:
        sys.exit("No results recieved from Spotify")

    # Creating the Album Data Structure:
    album_list = []
    for row in recently_played['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_tracks, 'url': album_url}
        album_list.append(album_element)

    # Creating the Artist Data Structure:
    # As we can see here this is another way to store data with using a dictionary of lists. Personally, for this project
    # I think using the strategy with the albums dicts(lists) is better. It allows for more functionality if we have to sort for example.
    # Additionally we do not need to make the temporary lists. There may be a more pythonic method to creating this but it is not my preferred method
    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played['items']:
        for key, value in item.items():
            if key == "track":
                for data_point in value['artists']:
                    id_list.append(data_point['id'])
                    name_list.append(data_point['name'])
                    url_list.append(data_point['external_urls']['spotify'])
    artist_dict = {'artist_id': id_list, 'name': name_list, 'url': url_list}
    # print(artist_dict)
    # Creating the Track(Song) Data Structure:
    song_list = []
    f = open('yrb.txt', 'w')
    for row in recently_played['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        f.write(song_name)
        f.write("\n")
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_time_played = row['played_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
                        'popularity': song_popularity, 'date_time_played': song_time_played, 'album_id': album_id,
                        'artist_id': artist_id
                        }
        song_list.append(song_element)
    f.close()
    # print(song_list)
    # Now that we have these two lists and one dictionary ready lets convert them to DataFrames
    # We will need to do some cleaning and add our Unique ID for the Track
    # Then load into PostgresSQL from the dataframe

    # Album = We can also just remove duplicates here. We dont want to load two of the same albums just to have SQL drop it later
    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])

    # Artist = We can also just remove duplicates here. We dont want to load two of the same artists just to have SQL drop it later
    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])

    # Song Dataframe
    song_df = pd.DataFrame.from_dict(song_list)
    # date_time_played is an object (data type) changing to a timestamp
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    # converting to my timezone of Central
    song_df['date_time_played'] = song_df['date_time_played'].dt.tz_convert(
        'US/Central')
    # I have to remove the timezone part from the date/time/timezone.
    song_df['date_time_played'] = song_df['date_time_played'].astype(
        str).str[:-7]
    song_df['date_time_played'] = pd.to_datetime(song_df['date_time_played'])
    # Creating a Unix Timestamp for Time Played. This will be one half of our unique identifier
    song_df['UNIX_Time_Stamp'] = (
        song_df['date_time_played'] - pd.Timestamp("1970-01-01"))//pd.Timedelta('1s')
    # I need to create a new unique identifier column because we dont want to be insterting the same song played at the same song
    # I can have the same song multiple times in my database but I dont want to have the same song played at the same time
    song_df['unique_identifier'] = song_df['song_id'] + \
        "-" + song_df['UNIX_Time_Stamp'].astype(str)
    song_df = song_df[['unique_identifier', 'song_id', 'song_name', 'duration_ms',
                       'url', 'popularity', 'date_time_played', 'album_id', 'artist_id']]
    song_df.to_csv(r"CSV\test1.csv")

    # LOADING IN POSTGREsql TEMP TABLE
    conn = psycopg2.connect(host="localhost", user="postgres",
                            password="yrm222829", port="5432", dbname="yrb")
    cur = conn.cursor()
    engine = create_engine(
        'postgresql+psycopg2://postgres:yrm222829@localhost/yrb')
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()
    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')

    # TRACKS: Temp Table
    cur_eng.execute(
        """
    CREATE TEMP TABLE IF NOT EXISTS tmp_track AS SELECT * FROM spotify_schema.spotify_track LIMIT 0
    """)
    song_df.to_sql("tmp_track", con=engine, if_exists='append', index=False)
    # Moving data from temp table to production table
    cur.execute(
        """
    INSERT INTO spotify_schema.spotify_track
    SELECT tmp_track.*
    FROM   tmp_track
    LEFT   JOIN spotify_schema.spotify_track USING (unique_identifier)
    WHERE  spotify_schema.spotify_track.unique_identifier IS NULL;
    
    DROP TABLE tmp_track""")
    conn.commit()

    # ALBUM: Temp Table
    cur_eng.execute(
        """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_schema.spotify_album LIMIT 0
    """)
    album_df.to_sql("tmp_album", con=engine, if_exists='append', index=False)
    conn_eng.commit()
    # Moving from Temp Table to Production Table
    cur.execute(
        """
    INSERT INTO spotify_schema.spotify_album
    SELECT tmp_album.*
    FROM   tmp_album
    LEFT   JOIN spotify_schema.spotify_album USING (album_id)
    WHERE  spotify_schema.spotify_album.album_id IS NULL;
    
    DROP TABLE tmp_album""")
    conn.commit()

    # Artist: Temp Table
    cur_eng.execute(
        """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_schema.spotify_artists LIMIT 0
    """)
    artist_df.to_sql("tmp_artist", con=engine, if_exists='append', index=False)
    conn_eng.commit()
    # Moving data from temp table to production table
    cur.execute(
        """
    INSERT INTO spotify_schema.spotify_artists
    SELECT tmp_artist.*
    FROM   tmp_artist
    LEFT   JOIN spotify_schema.spotify_artists USING (artist_id)
    WHERE  spotify_schema.spotify_artists.artist_id IS NULL;
    
    DROP TABLE tmp_artist""")
    conn.commit()
    print("Finished Extract, Transform, Load - Spotify")

def enrich_playlist(sp, username, playlist_id, playlist_tracks):
    index = 0
    results = []
    
    while index < len(playlist_tracks):
        results += sp.user_playlist_add_tracks(username, playlist_id, tracks = playlist_tracks[index:index + 50])
        index += 50


def generate_playlist_df(playlist_name, playlist_dic, spotify_data , sp):
    
    playlist = pd.DataFrame()
    
    for i, j in enumerate(sp.playlist(playlist_dic[playlist_name])['tracks']['items']):
        playlist.loc[i, 'artist'] = j['track']['artists'][0]['name']
        playlist.loc[i, 'track_name'] = j['track']['name']
        playlist.loc[i, 'track_id'] = j['track']['id']
        # playlist.loc[i, 'url'] = j['track']['album']['images'][0]['url']
        playlist.loc[i, 'date_added'] = j['added_at']

    playlist['date_added'] = pd.to_datetime(playlist['date_added'])  
    
    playlist = playlist[playlist['track_id'].isin(spotify_data['track_id'].values)].sort_values('date_added',ascending = False)
    return playlist

def generate_playlist_vector(spotify_features, playlist_df, weight_factor):
    
    spotify_features_playlist = spotify_features[spotify_features['track_id'].isin(playlist_df['track_id'].values)]
    spotify_features_playlist = spotify_features_playlist.merge(playlist_df[['track_id','date_added']], on = 'track_id', how = 'inner')
    
    spotify_features_nonplaylist = spotify_features[~spotify_features['track_id'].isin(playlist_df['track_id'].values)]
    
    playlist_feature_set = spotify_features_playlist.sort_values('date_added',ascending=False)
    
    
    most_recent_date = playlist_feature_set.iloc[0,-1]
    
    for ix, row in playlist_feature_set.iterrows():
        playlist_feature_set.loc[ix,'days_from_recent'] = int((most_recent_date.to_pydatetime() - row.iloc[-1].to_pydatetime()).days)
        
    
    playlist_feature_set['weight'] = playlist_feature_set['days_from_recent'].apply(lambda x: weight_factor ** (-x))
    
    playlist_feature_set_weighted = playlist_feature_set.copy()
    
    playlist_feature_set_weighted.update(playlist_feature_set_weighted.iloc[:,:-3].mul(playlist_feature_set_weighted.weight.astype(int),0))   
    
    playlist_feature_set_weighted_final = playlist_feature_set_weighted.iloc[:, :-3]
    

    
    return playlist_feature_set_weighted_final.sum(axis = 0), spotify_features_nonplaylist

def generate_recommendation(spotify_data, playlist_vector, nonplaylist_df , sp):
    from sklearn.metrics.pairwise import cosine_similarity


    non_playlist = spotify_data[spotify_data['track_id'].isin(nonplaylist_df['track_id'].values)]
    non_playlist['sim'] = cosine_similarity(nonplaylist_df.drop(['track_id'], axis = 1).values, playlist_vector.drop(labels = 'track_id').values.reshape(1, -1))[:,0]
    non_playlist_top15 = non_playlist.sort_values('sim',ascending = False).head(15)
    non_playlist_top15['url'] = non_playlist_top15['track_id'].apply(lambda x: sp.track(x)['album']['images'][1]['url'])
    
    return  non_playlist_top15

def recommend_songs():
    client_id = "b7ed80b8ba7e4e0297457b4063c3a375"
    client_secret = "53bc721903df42caad1964b87821ddd2"
    scope = 'playlist-modify-public user-read-recently-played user-top-read'
    username='qy898dz5521ijiuw4ih2l00qu'
    token = util.prompt_for_user_token(username , scope, client_id= client_id, client_secret=client_secret, redirect_uri='http://localhost:8881/callback')
    sp = spotipy.Spotify(auth=token)
    recently_played = sp.current_user_recently_played(limit=50)
    if len(recently_played) == 0:
        sys.exit("No results recieved from Spotify")
    
    #Creating Playlist
    playlist_name = "Recently Played"
    sp.user_playlist_create(username,name=playlist_name)
    print("Playlist Created.")
    print(playlist_name)

    playlist_name_recommend = "Recommendation using Python"
    sp.user_playlist_create(username,name=playlist_name_recommend)
    
    #Finding PlaylistID
    playlist_id = ''
    playlists = sp.user_playlists(username)
    # print(playlists)
    for playlist in playlists['items']:  # iterate through playlists I follow
        if playlist['name'] == playlist_name:  # filter for newly created playlist
            playlist_id = playlist['id']
    # print("Got Playlist ID.")
    print(playlist_id)

    # playlist_id = ''
    playlist_name_recommend = "Recommendation using Python"
    playlists = sp.user_playlists(username)
    # print(playlists)
    for playlist in playlists['items']:  # iterate through playlists I follow
        if playlist['name'] == playlist_name_recommend:  # filter for newly created playlist
            playlist_id_recommend = playlist['id']
    
    #Adding Recently PLayed songs to playlist
    for row in recently_played['items']:
        song_uri=row['track']['uri']
        sp.user_playlist_add_tracks(username, playlist_id,[song_uri])
    
    playlist_dic = {}
    playlist_cover_art = {}
    
    #Creating Playlist dictionary
    for i in sp.current_user_playlists()['items']:

        playlist_dic[i['name']] = i['uri'].split(':')[2]
        # playlist_cover_art[i['uri'].split(':')[2]] = i['images'][0]['url']    
    
    
    spotify_data = pd.read_csv('D:\SEMESTER6\BI\JCOMP\ETL\SpotifyFeatures.csv')
    playlist_df = generate_playlist_df("Recently Played", playlist_dic, spotify_data , sp)
    playlist_df.to_csv('playlist.csv')


    #Feature Engineering
    spotify_features_df = spotify_data
    genre_OHE = pd.get_dummies(spotify_features_df.genre)
    key_OHE = pd.get_dummies(spotify_features_df.key)
    from sklearn.preprocessing import MinMaxScaler
    scaled_features = MinMaxScaler().fit_transform([spotify_features_df['acousticness'].values,spotify_features_df['danceability'].values,spotify_features_df['duration_ms'].values,spotify_features_df['energy'].values,spotify_features_df['instrumentalness'].values,spotify_features_df['liveness'].values,spotify_features_df['loudness'].values,spotify_features_df['speechiness'].values,spotify_features_df['tempo'].values,spotify_features_df['valence'].values])
    spotify_features_df[['acousticness','danceability','duration_ms','energy','instrumentalness','liveness','loudness','speechiness','tempo','valence']] = scaled_features.T
    spotify_features_df = spotify_features_df.drop('genre',axis = 1)
    spotify_features_df = spotify_features_df.drop('artist_name', axis = 1)
    spotify_features_df = spotify_features_df.drop('track_name', axis = 1)
    spotify_features_df = spotify_features_df.drop('popularity',axis = 1)
    spotify_features_df = spotify_features_df.drop('key', axis = 1)
    spotify_features_df = spotify_features_df.drop('mode', axis = 1)
    spotify_features_df = spotify_features_df.drop('time_signature', axis = 1)

    spotify_features_df = spotify_features_df.join(genre_OHE)
    spotify_features_df = spotify_features_df.join(key_OHE)

    playlist_vector, nonplaylist_df = generate_playlist_vector(spotify_features_df, playlist_df, 1.2)
    top15 = generate_recommendation(spotify_data, playlist_vector, nonplaylist_df , sp)
    from string import digits
    remove_digits = str.maketrans('', '', digits)
    a=str(top15['track_name'])
    res = a.translate(remove_digits)
    import textwrap

    text=textwrap.dedent(res)
    print(text)
    f = open('yash.txt', 'w')
    f.writelines(text)

    list_track = top15['track_id']
    enrich_playlist(sp, username, playlist_id_recommend, list_track)

spotify_etl_func()
recommend_songs()
