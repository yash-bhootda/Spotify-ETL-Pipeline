import spotipy
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util


client_id = "b7ed80b8ba7e4e0297457b4063c3a375"
client_secret = "53bc721903df42caad1964b87821ddd2"
scope = 'playlist-modify-public user-read-recently-played user-top-read ugc-image-upload'
username='qy898dz5521ijiuw4ih2l00qu'
token = util.prompt_for_user_token(username , scope, client_id= client_id, client_secret=client_secret, redirect_uri='http://localhost:8080')
sp = spotipy.Spotify(auth=token)

playlist_name_recommend = "Recommendation using Python"
playlists = sp.user_playlists(username)
    # print(playlists)
for playlist in playlists['items']:  # iterate through playlists I follow
    if playlist['name'] == playlist_name_recommend:  # filter for newly created playlist
        playlist_id_recommend = playlist['id']
print(playlist_id_recommend)

import base64
with open('D:\SEMESTER6\BI\JCOMP\pexels-alem-sánchez-2760229.jpg', 'rb') as image_file:
    encoded_image = base64.b64encode(image_file.read())

sp.playlist_upload_cover_image(playlist_id_recommend, encoded_image)
