# Project Title

In this project, we want to extract relevant analytics information(song plays) from json files (songs files and users log files).
The proposed solution uses a star schema database where we have 4 dimension tables (time, users, songs and artists)
and one fact table (song_plays).

![alt text](https://i.ibb.co/rvpXBXD/star-schema.png)

The data is first copied to an Amazon Redshift cluster staging tables (staging_events, staging_songs) and then we use an etl to move the data we need to the star schema tables.
From there we can use the star schema tables as OLAP cubes for analytics purposes.

## Project files
The **dwh.cfg** file contains the configuration of your redshift cluser, you need to provide your redshift arn, database name, user name, password and port (default is 5439)

The **create_tables.py** script creates the staging_events and staging_songs tables and the star schema tables as shown on the image above.

The **sql_queries.py** contains simple queries for creating tables, saving data to the tables.

The **etl.py** script COPIES the json files to the staging tables, then TRANSFORMS the data and LOADS it to the star schema tables.

The **README.md** is the current file

## How to run
- First make sure that the cwh.cfg is well configured, you need to provide your redshift arn,  database name, user name, password and port

- run create_tables.py in order to create the tables, this command ensures that the tables 
will be re-created if they were already existing.

```~$ python3 create_tables.py```

- run etl.py to copy the data to your redshift database, this may take a while, so be patient

```~$ python3 etl.py```


At this point you should have all the data loaded in a star schema fashion and you can start doing some analytics on it, have fun !
