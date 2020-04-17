# Project 3: AWS Data Warehouse
---

This project is part of Data Engineering Nanodegree at Udacity.

### Project Description
---

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My task is to:
- Load data from S3 to staging tables on Redshift
- Execute SQL statements that create analytics tables from the staging tables

### Database schema
---

###### 2 Staging Tables to load data from S3
 - **staging_events**
 - **staging_songs**

###### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page "NextSong"

###### Dimension Tables
2. **users** - users in the app
3. **songs** - songs in music database
4. **artists** - artists in music database
5. **time** - timestamps of records in **songplays** broken down into specific units

<Database schema.png>

### Project requirements
---

* AWS account with master IAM role that has `AmazonRedshiftFullAccess` and `AdministratorAccess`
* Dependencies installed listed on *requirements.txt*

### Project file structure
---

* `create_Redshift.py`: Create a Redshift cluster using master IAM role specified in `masterIAM.cfg`
* `create_tables.py`: Create staging tables and analytics tables.
* `etl.py`: Parse data from S3 to staging tables and from staging tables to analytics tables.
* `sql_queries.py`: Stores SQL executions.
* `psycopg_connect.py`: To access PostgreSQL-based Redshift database.
* `dwh.cfg`: Stores cluster configuration and source data.
* `masterIAM.cfg`: Stores KEY and SECRET of master IAM.


### How to run
---

1. Create a Python environment with dependencies listed on `requirements.txt`.

2. Fill out information in config/dwh.cfg, except `[HOST]` and `[DWH_ROLE_ARN]`.
```
[CLUSTER]
DWH_DB=sparkify
DWH_CLUSTER_IDENTIFIER=
DWH_USER=
DWH_PASSWORD=
DWH_PORT=5439
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_IAM_ROLE_NAME=dwhRole
HOST=warehouseuda3.cv8hhy4btnl1.us-west-2.redshift.amazonaws.com
DWH_ROLE_ARN=arn:aws:iam::589450477615:role/dwhRole

[S3]
LOG_DATA='s3://udacity-dend/log-data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song-data'
```
3. Fill out master IAM role information in `config/masterIAM.cfg`.
```
[KEY]
KEYID=
SECRET_ACCESS=
```

4. Run `create_Redshift.py`.
```python
$ python create_redshift.py
```
5. Copy information printed out from `create_Redshift.py` and fill out `[HOST]` and `[DWH_ROLE_ARN]` in `dwh.cfg`.

6. Run `create_tables.py`.
```python
$ python create_tables.py`
```

7. Run `etl.py`.
```python
$ python etl.py
```





