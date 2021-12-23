# data_warehouse
An ETL pipeline for a database hosted on Redshift.

# Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This projects build an ETL pipeline, extracts the data from S3, stages the data in redshift and transfroms the data into a set of dimension tables. The fact and dimension tables that are built allow the analytical team to continue finding insights about songs that users listen to.

# Database schema design
## Staging tables
staging_events and staging_songs are the two staging tables to store data loaded from S3. 

## Fact table
songplay is the fact table that stores event data associated with song plays. 
songplay_id is the primary key; artist_id is set to be both sortkey and distkey as it is most likely to be the key column to join with other tables, and we would like to perform filtering based on these values.

## Dimension tables
- users: users that use the app
All distribution style is chosen for the users table, this is because we need to make joins with the users from the songplays table. We expect that the users table will not grow too large and the users will have slowly changing dimensions.
- songs: songs in the music database
song_id is the primary key, it is also the sort key as we would likely to perform filtering on song_id. artist_id is chosen to be the distkey to match the distkey we set in the fact table.
- artists: artists in the music database
artist_id is chosen to be the primary key, sort key and distkey. It is chosen as the primary key as it will make each record unique; we will be likely to filter the data according to artist_id so it is chosen to be the sortkey. It is also chosen to be the distkey to match the distkey for the fact table.
- time: timestamp of records in songplay
Starttime is chosen to be the primary key; diststyle all is chosen for the table, as the table is "small enough" to be distributed with an all strategy.

