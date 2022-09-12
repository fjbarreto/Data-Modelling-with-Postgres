# Data modeling with Postgres 

In this first **Udacity data engineering nanodegree project** we must create a postgres database schema and an ETL pipeline for a music streaming platform called **Sparkify**. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


# Schema design 

For this project a star schema with one **fact** table (songplays) and 4 **dimension** tables (artists, songs, users and time) is used. This schema design will minimize the need for JOIN statements, allowing faster reads of the analytics database. 
![Schema](https://user-images.githubusercontent.com/97537153/189656630-3b5373a3-b989-4480-975a-7958938e607f.png)

# Project files

• **sql_queries.py**: python script with CREATE, DROP and INSERT statements needed.

• **create_tables.py**: python script that executes drop and create statements from **sql_queries.py** in our postgres db.

• **etl.py**: python script with user defined functions that extract, transform and loads data into our tables from the JSON files.
