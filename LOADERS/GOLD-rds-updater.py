"""
AWS Glue ETL Script for creating or updating auxiliary RDS tables used to serve the application.
"""

### LIBRARIES
import ast
import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import scipy
import sys
import time

from math import ceil
from s3fs import S3FileSystem
from scipy.stats import zscore

from awsglue.utils import getResolvedOptions 

### WARNING
# NB: current behaviour is to run for ALL unique db_ids in the clients list

### CONFIG ###
# Retrieve client id passed from STEP FUNCTION map as parameter
args = getResolvedOptions(sys.argv, ['db_cluster_arn', 'db_credentials_secret_store_arn'])


# Define RDS access credentials
rds_client = boto3.client('rds-data')
database_name = 'traitsproddb'
db_cluster_arn = args['db_cluster_arn']
db_credentials_secret_store_arn = args['db_credentials_secret_store_arn']
s3 = S3FileSystem()

# Retrieve any competition restrictions
# s3 = S3FileSystem()
clients = pd.read_csv('s3://traits-app/settings/clients.csv')
db_ids = [int(i) for i in list(set(clients['db_id'].loc[~clients['db_id'].isna()]))]
# clients.index = clients['id'].astype('int')
# try:
#     db_id = clients.at[int(client_id), 'db_id']
# except:
#     db_id = client_id
# reset_data = clients.at[int(client_id), 'reset_data']
# try:
#     use_ids = 1
#     leagues = ast.literal_eval(clients.at[int(client_id), 'league_ids'])
# except:
#     try:
#         use_ids = 0
#         leagues = ast.literal_eval(clients.at[int(client_id), 'league_names'])
#     except:
#         use_ids = 0 
#         leagues = False

def execute_statement(sql, parameters=None):
    ### Executes an SQL statement using AWS RDS Data API with optional parameters.
    if parameters is not None:
        response = rds_client.execute_statement(
            secretArn=db_credentials_secret_store_arn,
            database=database_name,
            resourceArn=db_cluster_arn,
            sql=sql,
            parameters=parameters
        )
    else:
        response = rds_client.execute_statement(
            secretArn=db_credentials_secret_store_arn,
            database=database_name,
            resourceArn=db_cluster_arn,
            sql=sql
        )
    return response
    
print("Creating fact tables (if not exists).")
# TODO: logic should efficiently check if ALL aux tables exist, and if there are any updates to team, competition, nationality, season. For now it just reruns every time.
# Or at least truncate and rewrite
def gen_table_statements(db_id):
    return [
        """CREATE TABLE IF NOT EXISTS `id_{db_id}`.competition (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );""".format(db_id=db_id), 
        """INSERT IGNORE INTO `id_{db_id}`.competition (id, name) 
            SELECT DISTINCT competitionId, competitionName 
            FROM `id_{db_id}`.Output;""".format(db_id=db_id),
        """CREATE TABLE IF NOT EXISTS `id_{db_id}`.season (
            name VARCHAR(255) NOT NULL PRIMARY KEY
        );""".format(db_id=db_id),
        """INSERT IGNORE INTO `id_{db_id}`.season (name) 
            SELECT DISTINCT seasonName 
            FROM `id_{db_id}`.Output;""".format(db_id=db_id),
        """CREATE TABLE IF NOT EXISTS `id_{db_id}`.nationality (
            name VARCHAR(255) NOT NULL PRIMARY KEY
        );""".format(db_id=db_id),
        """INSERT IGNORE INTO `id_{db_id}`.nationality (name) 
            SELECT DISTINCT nationality 
            FROM `id_{db_id}`.Output;""".format(db_id=db_id),
        """CREATE TABLE IF NOT EXISTS `id_{db_id}`.team (
            name VARCHAR(255) NOT NULL COLLATE utf8mb4_unicode_ci,
            competitionId VARCHAR(255) NOT NULL,
            CONSTRAINT fk_competition
                FOREIGN KEY (competitionId) REFERENCES `id_{db_id}`.competition(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT unique_team_competition UNIQUE (name, competitionId)
        );""".format(db_id=db_id),
        """INSERT IGNORE INTO `id_{db_id}`.team (name, competitionId)
            SELECT DISTINCT teamName, competitionId 
            FROM `id_{db_id}`.Output;""".format(db_id=db_id)
    ]

print(db_ids)
for db_id in db_ids:
    if db_id != "nan" and db_id != "":
        print("Updating fact tables with database id {}.".format(db_id))
        for sql in gen_table_statements(db_id=db_id):
            execute_statement(sql)