from recommend import recommend_songs
from spotify_etl import spotify_etl_func
from Weekly_Email import weekly_email_function

spotify_etl_func()
recommend_songs()
weekly_email_function()