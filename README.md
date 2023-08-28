# Spotify-ETL-Pipeline
# Data Engineering Project-2|Building Spotify ETL using Python and Airflow

Create an Extract Transform Load pipeline using python and automate with airflow.

![](https://miro.medium.com/max/749/1*dm8hVrPTPMenyRY4uJiBIA@2x.png)


# Problem Statement:

We need to use Spotify’s API to read the data and perform some basic transformations and Data Quality checks finally will load the retrieved data to PostgreSQL DB and then automate the entire process through airflow.  **Est.Time:**[4–7 Hours]

# Tech Stack / Skill used:

1.  Python
2.  API’s
3.  Docker
4.  Airflow
5.  PostgreSQL

# Prerequisite:

1.  Knowledge on API
2.  Understand what docker and docker-compose
3.  Intermediate Python and SQL
4.  A basic understanding of Airflow  [this](https://www.youtube.com/watch?v=AHMm1wfGuHE&t=705s) will help

# Learning Outcomes:

1.  Understand how to interact with API to retrieve data
2.  Handling Dataframe in pandas
3.  Setting up Airflow and PostgreSQL through Docker-Compose.
4.  Learning to Create DAGs in Airflow

# Introduction:

This is a beginner-friendly project to get started with building a simple pipeline and automating through airflow. First, we will focus on entirely building the pipeline and then extend the project by combining it with Airflow.

# Building ETL Pipeline:

**Dataset:** In this project, we are using Spotify’s API so please go ahead and create an account for yourself. After creating the account head to this  [page](https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=). Now you will be able to see a get token icon click that and select user recently played and click get token.

## Extract.py

We are using this token to Extract the Data from Spotify. We are Creating a function return_dataframe(). The Below python code explains how we extract API data and convert it to a Dataframe.

## Transform.py

Here we are exporting the Extract file to get the data.

**def Data_Quality(load_df):** Used to check for the empty data frame, enforce unique constraints, checking for null values. Since these data might ruin our database it's important we enforce these Data Quality checks.

**def Transform_df(load_df):** Now we are writing some logic according to our requirement here we wanted to know our favorite artist so we are grouping the songs listened to by the artist. Note: This step is not required you can implement it or any other logic if you wish but make sure you enforce the primary constraint.

## Load.py

In the load step, we are using sqlalchemy and SQLite to load our data into a database and save the file in our project directory.

Finally, we have completed our ETL pipeline successfully. The structure of the project folder should look like this(inside the project folder we have 3 files).

E:\DE\PROJECTS\SPOTIFY_ETL\SPOTIFY_ETL  
│   Extract.py  
│   Load.py  
│   my_played_tracks.sqlite  
│   spotify_etl.py  
│   Transform.py  
└───

## spotify_etl.py

In this Python File will write a logic to extract data from API → Do Quality Checks →Transform Data.

1.  **yesterday = today — datetime.timedelta(days=1)**  → Defines the number of days you want data for, change as you wish since our job is the daily load I have set it to 1.
2.  **def spotify_etl()**  → Core function which returns the Data Frame to the DAG python file.
3.  This file needs to be placed inside the dags folder

## spotify_final_dag.py

This is the most important section you need to pay attention to. First, learn the basics about airflow DAG’s  [here](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)  it might take around 15 mins or you can search for it on youtube. After the basics please follow the below guideline.

1.  **from airflow.operators.python_operator import PythonOperator**  → we are using the python operator to perform python functions such as inserting DataFrame to the table.
2.  **from airflow.providers.postgres.operators.postgres import PostgresOperator**  → we are using the Postgres operator to create tables in our Postgres database.
3.  **from airflow. hooks.base_hook import BaseHook**  → A hook is an abstraction of a specific API that allows Airflow to interact with an external system. Hooks are built into many operators, but they can also be used directly in DAG code. We are using a hook here to connect Postgres Database from our python function
4.  **from spotify_etl import spotify_etl**  → Importing spotify_etl function from spotify_etl.py
