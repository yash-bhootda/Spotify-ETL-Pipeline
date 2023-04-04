import pandas as pd 
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import sys
from io import StringIO

from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util

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
    client_id = ""
    client_secret = ""
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
