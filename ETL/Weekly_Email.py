
# This is the code for the weekly Spotify Wrap Up Email
import psycopg2
import smtplib
import ssl
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tabulate import tabulate
from datetime import datetime, timedelta


def weekly_email_function():
    conn = psycopg2.connect(host="localhost", user="postgres",
                            password="yrm222829", port="5432", dbname="yrb")

    cur = conn.cursor()
    today = datetime.today().date()
    six_days_ago = today - timedelta(days=6)
    # with open('D:\SEMESTER 6\BI\JCOMP\yash.txt', 'r') as file:
    #     data = file.read()
    # data1=[[data]]
    my_file = open("D:\SEMESTER6\BI\JCOMP\yash.txt", "r")
    list=[]
    data = my_file.read()
    for i in range(15):
        list.append(data[55*i:55*(i+1)])

    # replacing end of line('/n') with ' ' and
    # splitting the text it further when '.' is seen.
    data_into_list = data.replace(' ', ' ').split(".")

    # printing the data
    # print(data_into_list[0])
    my_file.close()



    # Top 5 Songs by Time Listened (MIN)
    top_5_songs_min = [['Song Name', 'Time (Min)']]
    cur.callproc('function_last_7_days_top_5_songs_duration2')
    for row in cur.fetchall():
        song_name = row[0]
        min_listened = float(row[1])
        element = [song_name, min_listened]
        top_5_songs_min.append(element)

    # Total Time Listened (HOURS)
    cur.callproc('function_last_7_days_hrs_listened')
    total_time_listened_hrs = float(cur.fetchone()[0])

    # Top 5 Songs and Artists by Times Played
    top_songs_art_played = [['Song Name', 'Arist Name', 'Times Played']]
    cur.callproc('function_last_7_days_songs_artist_played')
    for row in cur.fetchall():
        song_name = row[0]
        artist_name = row[1]
        times_played = int(row[2])
        element = [song_name, artist_name, times_played]
        top_songs_art_played.append(element)

    # Top Artists Played
    top_art_played = [['Artist Name', 'Times Played']]
    cur.callproc('function_last_7_days_artist_played')
    for row in cur.fetchall():
        artist_name = row[0]
        times_played = int(row[1])
        element = [artist_name, times_played]
        top_art_played.append(element)

    # Top Decades:
    top_decade_played = [['Decade', 'Times Played']]
    cur.callproc('function_last_7_days_top_decades1')
    for row in cur.fetchall():
        decade = row[0]
        times_played = int(row[1])
        element = [decade, times_played]
        top_decade_played.append(element)

    # Sending the Email:
    port =465
    password = "sjjfzfxthnrtozdf"

    sender_email = "yrbhootda@gmail.com"
    receiver_email = "yrbhootda@gmail.com"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"Spotify - Weekly Roundup - {today}"
    message["From"] = sender_email
    message["To"] = receiver_email

    text = f"""\
    Here are your stats for your weekly round up for Spotify. 
    Dates included: {six_days_ago} - {today}:
    
    Total Time Listened: {total_time_listened_hrs} hours.
    You listened to these songs and artists a lot here are your top 5!
    {top_songs_art_played}
    You spent the most time listening to these songs:
    {top_5_songs_min}
    You spend the most time listening to these artists:
    {top_art_played}
    Lastly your top decades are as follows:
    {top_decade_played}
    """
    html = f"""\
    <html>
        <body>
            <h4>
            Here are your stats for your weekly round up for Spotify.
            </h4>
            <p>
            Dates included: {six_days_ago} - {today}
            <br>
            Total Time Listened: {total_time_listened_hrs} hours.
            <br>
            <h4>
            You listened to these songs and artists a lot here are your top 5!
            </h4>
            {tabulate(top_songs_art_played, tablefmt='html')}
            <h4>
            You spend a lot of time listening to these songs!
            </h4>
            {tabulate(top_5_songs_min, tablefmt='html')}
            <h4>
            You spend a lot of time listening to these artists!
            </h4>
            {tabulate(top_art_played, tablefmt='html')}
            <h4>
            Lastly your top decades are as follows:
            </h4>
            {tabulate(top_decade_played, tablefmt='html')}
            <h4> Your top recommendations</h4>
            <table>
            <tr> <th> Track Name </th></tr>
            <tr> <td> {list[0]} </td> </tr>
            <tr> <td> {list[1]} </td> </tr>
            <tr> <td> {list[2]} </td> </tr>
            </table>
            # {list[1]}
            # </h5>
            </p>
            
        </body>
    </html>"""

    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    part3 = MIMEText(data, "plain")

    message.attach(part1)
    # message.attach(part3)
    message.attach(part2)

    filename = "Recommendation"
    # f = file(filename)
    attachment = MIMEText(data)
    attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
    message.attach(attachment)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login("yrbhootda@gmail.com", password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    print("Email Sent")

weekly_email_function()