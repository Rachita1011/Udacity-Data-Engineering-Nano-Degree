# Project 3: Data Warehouse

## Introduction 
A music streaming startup company called Sparkify wants to move their processes and data onto the cloud. They have collected data on songs and the user activity of customers using Sparkify. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They would like a data engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project Description
The purpose of the project is to build an ETL pipeline for a database that is hosted on Amazon Redshift. Data will be loaded into S3 via staging tables on Redshift. SQL statements will be executed to create the analytical tables that would be used by Sparkify. The star schema is used to build fact and dimension tables that can be used by Sparkify to carry out any analyse data easily and efficiently. 

## Project Files

1. `create _tables.py` - This script is to be run to drop any old tables if they exist on the redshift cluster, and then to recreate all the tables   
2. `dwh.cfg` - This file contains all the credential information for the AWS resources, including database name, arn, region, as well as the bucket location in S3 for the datasets.
3. `sql_queries.py` - This script contains all the sql statements that go on to create the tables, extract the data from redshift, load into staging tables and insert into the final tables. 
4. `etl.py` - This script is run to load the raw data from the S3 buckets to the Amazon Redshift staging tables specified in the sql_queries.py script. It also then goes on to insert the data from the staging tables into the fact and dimension tables detailed below.

## Datasets Used

### Song Dataset

The first dataset is a subset of data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) . Each file is in JSON format and contains metadata about a song and the artist of that song. 

And below is an example of what a single song file looks like:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

This dataset is hosted at S3 bucket: 

```
s3://udacity-dend/song_data
```

### Log Dataset 

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from the music streaming app based on specific configuration settings.

```
s3://udacity-dend/log_data
```

## Staging Tables

Two staging tables were created. Staging_events and staging songs. Staging_events table contains the data from S3 bucket for the log data
and staging_songs table contains the data from S3 bucket for the songs data.

Their schemas are as follows:

```
Staging_events:

        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INT
        
Staging_songs:

        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
```

## Database Schema

The star schema was used in designing the database for this project. Of which the schema contains five tables: one of which is a fact table and the rest are dimensional tables.

The schema is as follows:

### Fact Table:

#### Songplays

```
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP  REFERENCES time(start_time)    sortkey,
        user_id INT           REFERENCES users(user_id)      distkey,
        level VARCHAR,
        song_id VARCHAR       REFERENCES songs (song_id),
        artist_id VARCHAR     REFERENCES artist(artist_id),
        session_id INT        NOT NULL,
        location VARCHAR,
        user_agent VARCHAR

```

### Dimension Tables:

#### Users

```
        user_id INT          distkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
```

#### Artists

```
        artist_id VARCHAR    sortkey,
        name VARCHAR         NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
```

#### Songs

```
        song_id VARCHAR      sortkey,
        title VARCHAR        NOT NULL,
        artist_id VARCHAR    NOT NULL,
        year INT,
        duration FLOAT
```

#### Time

```
        start_time TIMESTAMP  sortkey,
        hour INT              NOT NULL,
        day INT               NOT NULL,
        week INT              NOT NULL,
        month INT             NOT NULL,
        year INT              NOT NULL,
        weekday INT           NOT NULL
```

## Example Queries

> - Get all songs from a specific year (for example 2000)

```
    SELECT  s.song_id,
            s.title,
            a.name,
            s.artist_id
    FROM songs as s
    JOIN artists as a
    ON s.artist_id = a.artist_id 
    WHERE s.year = 2000;
``` 

> - Total number of users by gender

```    
    SELECT  u.gender,
            COUNT(*)
    FROM users as u
    GROUP BY 1;
```