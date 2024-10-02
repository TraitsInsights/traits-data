import boto3
import ast
import datetime as dt
from dateutil import parser
import json
import pandas as pd
import requests
import sys
import time
from s3fs import S3FileSystem

from awsglue.utils import getResolvedOptions

"""Pull Statsbomb Data Feeds

This is a custom Python script for calling the Statbomb API player match stats endpoint then dumping the raw JSON files to S3 buckets.

By default, new matches are limited to matches with seasons ending in the <current_year> parameter, unless the <reset> parameter is explicitly passed.

Only matches with updates after the latest <refresh_date> are called.

S3 buckets follow the format <s3://traits-app/bronze-raw/statsbomb/{clientID}/{league}/{season}/{match_id}.json>

"""

# Helper functions
def execute_statement(sql, parameters=None):
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

# Retrieve arguments passed from STEP FUNCTION map as parameter
try:
    args = getResolvedOptions(sys.argv, ['STATSBOMB_AUTH', 'seasons', 'competitions', 'client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn'])
    client_id = args['client_id']
except:
    args = getResolvedOptions(sys.argv, ['STATSBOMB_AUTH', 'seasons', 'competitions', 'default_client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn'])
    client_id = args['default_client_id']
print(f"Client ID: {client_id}")

auth = args['STATSBOMB_AUTH']

# Expects a list of seasons that are to be checked for updates
try:
    comps_to_update = ast.literal_eval(args['competitions'])
except Exception as e:
    print(f"No competitions specified.", e)
    comps_to_update = None
seasons_to_update = ast.literal_eval(args['seasons'])

# Define RDS access credentials
rds_client = boto3.client('rds-data')
database_name = 'traitsproddb'
db_cluster_arn = args['db_cluster_arn']
db_credentials_secret_store_arn = args['db_credentials_secret_store_arn']

# # Retrieve statsbomb clients and basic auth keys
# # Execute the SQL query
# sql_query = "SELECT auth_basic, league_names, seasons FROM permissions WHERE id = :client_id"
# parameters = [{'name':'client_id', 'value':{'longValue': int(client_id)}}]
# response = execute_statement(sql_query, parameters)

# Process response
# try:
#     auth_basic = response['records'][0][0]['stringValue']
# except:
#     print("Missing or invalid auth key")
#     auth_basic = ''
# league_names = response['records'][0][1]['stringValue']
# seasons = response['records'][0][2]['stringValue']

# # Retrieve the last update timestamp
# # Define the SQL query
# sql_query = "SELECT MAX(timestamp) FROM updates WHERE client_id = :client_id"
# parameters = [{'name':'client_id', 'value':{'longValue': int(client_id)}}]
# response = execute_statement(sql_query, parameters)

# # Process response
# print(response['records'])
# try:
#     previous_call_unix = response['records'][0][3]['longValue']
# except:
#     # If no record found, set to Unix epoch start time (January 1, 1970)
#     print("Last refresh date could not be retrieved. Defaulting to all-time.")
#     previous_call_unix = 0
    
# previous_call = dt.datetime.utcfromtimestamp(previous_call_unix)

# print("Previous call:", dt.datetime.utcfromtimestamp(previous_call_unix).strftime('%Y-%m-%d %H:%M:%S'))

# Retrieve available league-seasons
comps_endpoint = "https://data.statsbombservices.com/api/v4/competitions"
payload={}
headers = {
  'Authorization': 'Basic {}'.format(auth)
}

try:
    response = requests.request("GET", comps_endpoint, headers=headers, data=payload)
    all_seasons_tup = [(item['competition_id'], item['competition_name'], item['season_id'], item['season_name'], item['match_updated']) for item in response.json()]
except requests.exceptions.RequestException as e:
    print("An error occurred while making the request to the Competitions endpoint:", e)
    raise

# Filter available leagues for Traits subscribed leagues
if comps_to_update != None:
    filtered_seasons = [tup for tup in all_seasons_tup if ((str(tup[2]) in seasons_to_update) and (str(tup[0]) in comps_to_update))]
else:
    filtered_seasons = [tup for tup in all_seasons_tup if (str(tup[2]) in seasons_to_update)]
print(comps_to_update)

# PREVIOUS SCRIPT FOR PLAYER-SEASON DATA 
# # call data for updated league-seasons
# all_data = []

# url2 = "https://data.statsbomb.com/api/v2/competitions/{comp_id}/seasons/{season_id}/player-stats"
# payload2={}
# headers2 = {
#   'Authorization': 'Basic {}'.format(auth)
# }

# for tup in parse_list:
#     try:
#         if previous_call < parser.parse(tup[2]):
#             print("updating league-season: ", tup[0], " ", tup[1])
#             data = requests.request("GET", url2.format(comp_id=tup[0],season_id=tup[1]), headers=headers2, data=payload2)
#             all_data = all_data + data.json()
#     except:
#         print("Incorrect format: ", tup)

# Retrive list of matches in competition-seasons to update
all_matches = []

matches_endpoint = "https://data.statsbombservices.com/api/v6/competitions/{comp_id}/seasons/{season_id}/matches"

for tup in filtered_seasons:
    # Return all matches in the comp-season
    match_data = requests.request("GET", matches_endpoint.format(comp_id=tup[0],season_id=tup[2]), headers=headers)
    match_data = match_data.json()
    # Filter for matches that have been updated since the last refresh
    for match in match_data:
        if match['collection_status']=='Complete' and match['match_status']=='available' and match['play_status']=='Normal':
            all_matches.append(match)
            
print(filtered_seasons)
print(f"No. matches: {len(all_matches)}")
print(all_matches[50])

# Retrieve match data from valid matches and write to S3 partition on league/season
s3 = S3FileSystem() # initiate file manager
matchstats_endpoint = "https://data.statsbombservices.com/api/v4/matches/{match_id}/player-stats"
lineups_endpoint = "https://data.statsbomb.com/api/v4/lineups/{match_id}"
events_endpoint = "https://data.statsbomb.com/api/v8/events/{match_id}"

# Log match ids and matches with errors
matches_ls = []
errors_ls = []

for match in all_matches:
    try:
        match_id = match['match_id']
        matches_ls.append(match_id) # Log match ids
        comp_id = str(match['competition']['competition_id'])
        season_id = str(match['season']['season_id'])
        comp_season_id = "0"+comp_id+season_id if len(comp_id)<3 else comp_id+season_id
        # Write match player stats json to S3
        stats_data = requests.request("GET", matchstats_endpoint.format(match_id = match_id), headers=headers).json()
        s3_path = 's3://traits-app/bronze-raw/statsbomb/{}/{}/matches/{}.json'.format(client_id, comp_season_id, match_id)
        with s3.open(s3_path, 'w', encoding='utf8') as file:
            json.dump(stats_data, file, ensure_ascii=False)
#         # Write lineups json to s3_path for the match
        lineups_data = requests.request("GET", lineups_endpoint.format(match_id = match_id), headers=headers).json()
        s3_path = 's3://traits-app/bronze-raw/statsbomb/{}/{}/lineups/{}.json'.format(client_id, comp_season_id, match_id)
        with s3.open(s3_path, 'w', encoding='utf8') as file:
            json.dump(lineups_data, file, ensure_ascii=False)
        # Write events json to s3_path for the match
        events_data = requests.request("GET", events_endpoint.format(match_id = match_id), headers=headers).json()
        s3_path = 's3://traits-app/bronze-raw/statsbomb/{}/{}/events/{}.json'.format(client_id, comp_season_id, match_id)
        with s3.open(s3_path, 'w', encoding='utf8') as file:
            json.dump(events_data, file, ensure_ascii=False)
    except Exception as e:
        print(e)
        errors_ls.append(match['match_id'])
        
if len(errors_ls) > 0:
    print(f"{len(errors_ls)} matches could not be saved. Check logs.")