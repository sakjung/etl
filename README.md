# Sparkify 

## Introduction

Sparkify is a virtual startup company that provides new music streaming app service. The Sparkify database mainly aims to collect the data on songs and user activity on the new music streaming app. The collected data can be used to obtain various insights about user preferences on songs and other further analyses.<br><br>
This project will build a data pipeline that implements ETL process from their JSON app data sources to the database. The final database schema will be formulated as `star schema`:

**Fact Table**
    
    1. songplays - records in log data associated with song plays i.e. records with page `NextSong`

        > songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent 
        
**Dimension Tables**

    2. users - users in the app
    
        > user_id, first_name, last_name, gender, level
    
    3. songs - songs in music database
    
        > song_id, title, artist_id, year, duration
    
    4. artists - artists in music database
    
        > artist_id, name, location, latitude, longitude
    
    5. times - timestamps of records in songplays broken down into specific units
    
        > start_time, hour, day, week, month, year, weekday
        

## sql_queries.py

This script contains all the **DROP**, **CREATE**, **INSERT** and **SELECT** queries related to the ETL process. This script will be imported to `etl.py` or `create_tables.py` scripts to be used.


## create_tables.py

This script will generate new database and table schema. In order to run this script run this code into the command shell:

```
python create_tabels.py
```

## etl.py

This script is the main script that actually implemenets ETL process. This script will read required JSON app data and put relevant data into the database which has been created by `create_tables.py`. In order to make sure that the code runs without error, several tests were implemented in `etl.ipynb`.
<br><br>
**`etl.py` MUST be run AFTER running `create_tables.py`.**

```
python etl.py
```

After running this code, all the required tasks are done. You can check if the data has been inserted without any problem with `test.ipynb`.


