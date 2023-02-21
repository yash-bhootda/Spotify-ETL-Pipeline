	

import pandas as pd
import requests
from datetime import datetime
import datetime


USER_ID = "qy898dz5521ijiuw4ih2l00qu"
TOKEN = "BQDeJKxc0Yl-boShOgnqPQUxzwr2nYVZD4XaC4WcxjiCs9VxPOw6uaI3DWiM3H_a5AsYUOiUHy9bppYwH-JfhtlW-VH65C5Z4MQwQsmGBmUEdushNk-xN4yDCG8FMIphS0C3JIW5_BYVv0UeFuS8o87aUYMylq93d5VeShmRIS0VCFsofME7y6PYPFU9_nc7j3JSOvBybg"


# Creating an function to be used in other python files
def return_dataframe():
    input_variables = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)  # no of Days u want the data for)
    yesterday_unix_timestamp=int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r=requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(
        time=yesterday_unix_timestamp), headers=input_variables)

    data=r.json()
    # print(data)
    song_names=[]
    artist_names=[]
    played_at_list=[]
    timestamps=[]

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    print(song_names)
    # Prepare a dictionary in order to turn it into a pandas dataframe below
    song_dict={
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }
    song_df=pd.DataFrame(song_dict, columns=[
                         "song_name", "artist_name", "played_at", "timestamp"])
    # print("uh")
    return song_df
return_dataframe()