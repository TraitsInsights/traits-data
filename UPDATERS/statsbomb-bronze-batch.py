### Libraries ###
import ast
import boto3
import concurrent
import datetime as dt
import json
import os
import pandas as pd
import requests
import sys
import time

from awsglue.utils import getResolvedOptions
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
from s3fs import S3FileSystem
from time import sleep

"""Pull Statsbomb Data Feeds

This is a custom Python script for calling the Statbomb API player match stats endpoint then dumping the raw JSON files to S3 buckets.

Matches to be parsed for these seasons are cross-checked with existing match data stored in S3. TODO: implement optional competitions and/or seasons parameters.

S3 buckets follow the format <s3://traits-app/bronze-raw/statsbomb/{clientID}/{league}/{season}/{match_id}.json>

"""

### CONFIG ###
args = getResolvedOptions(sys.argv, ['STATSBOMB_AUTH'])
# TODO: if seasons count becomes unruly, pass seasons_to_update as a parameter
auth = args['STATSBOMB_AUTH']

payload = {}
headers = {
  'Authorization': str(auth)
}

s3 = S3FileSystem() #Initiate file manager
b3 = boto3.client('s3')
    
### HELPERS ###
def list_s3_files(bucket_name, prefix):
    try:
        s3_response = b3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        parsed_files = []
        if 'Contents' in s3_response:
            # Collect files and their last modified times
            file_dict = {}
            for content in s3_response['Contents']:
                if content['Key'].endswith('.json') and content['Key'].split('/')[-1].replace('.json', '').isdigit():
                    match_id = int(content['Key'].split('/')[-1].replace('.json', ''))
                    if (match_id not in file_dict) or (content['LastModified'] > file_dict[match_id]['LastModified']):
                        file_dict[match_id] = {'Key': content['Key'], 'Size': content['Size'], 'LastModified': content['LastModified']}

            # Convert dictionary back to list
            parsed_files = [{'Key': value['Key'], 'Size': value['Size'], 'LastModified': value['LastModified']} for key, value in file_dict.items()]
    except b3.exceptions.NoSuchBucket:
        print(f"No such bucket: {bucket_name}")
        parsed_files = []
    except b3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No S3 key found for: {prefix}")
        parsed_files = []
    return parsed_files
    
# Helper function to fetch and save match data
def fetch_and_save_match_data(match, comp_season_id, headers):
    try:
        match_id = match['match_id']
        comp_id = str(match['competition']['competition_id'])
        season_id = str(match['season']['season_id'])
        
        # Fetch match player stats data and save to S3
        stats_data = requests.request("GET", matchstats_endpoint.format(match_id=match_id), headers=headers).json()
        s3_path = f's3://traits-app/bronze-raw/statsbomb/seasons/{comp_season_id}/players-stats/{match_id}.json'
        with s3.open(s3_path, 'w', encoding='utf8') as file:
            json.dump(stats_data, file, ensure_ascii=False)
        
        # Fetch lineups data and save to S3
        lineups_data = requests.request("GET", lineups_endpoint.format(match_id=match_id), headers=headers).json()
        s3_path = f's3://traits-app/bronze-raw/statsbomb/seasons/{comp_season_id}/lineups/{match_id}.json'
        with s3.open(s3_path, 'w', encoding='utf8') as file:
            json.dump(lineups_data, file, ensure_ascii=False)
        
        # OPTIONAL: uncomment for events
        # events_data = requests.request("GET", events_endpoint.format(match_id=match_id), headers=headers).json()
        # s3_path = f's3://traits-app/bronze-raw/statsbomb/seasons/{comp_season_id}/events/{match_id}.json'
        # with s3.open(s3_path, 'w', encoding='utf8') as file:
        #     json.dump(events_data, file, ensure_ascii=False)
        
        return match_id, 'success'
    
    except Exception as e:
        print(f"Error processing match {match['match_id']}: {e}")
        return match_id, 'error'
        
def process_season(compId, seasonId): # ADAPT This
    
    """Fetches player and team match stats based on a fixtures json file and saves individual resultss to s3 in a multi-threaded process."""
    
    compSeasonId = "0"+str(compId)+str(seasonId) if len(str(compId))<3 else str(compId)+str(seasonId) # create a unique competition-season ID
    
    print(f"Processing comp-season: {compSeasonId}")
    
    # Retrieve list of matches in competition-seasons to update
    logged_matches = []

    matches_endpoint = "https://data.statsbombservices.com/api/v6/competitions/{comp_id}/seasons/{season_id}/matches"
    
    # Return all matches in the comp-season
    match_data = requests.request("GET", matches_endpoint.format(comp_id=compId,season_id=seasonId), headers=headers)
    match_data = match_data.json()
    # Filter for matches with logged data
    for match in match_data:
        if match['collection_status']=='Complete' and match['match_status']=='available' and match['play_status']=='Normal':
            logged_matches.append(match)
                
    print(f"Available matches: {len(logged_matches)}")
    
    # Filter out matches already present to avoid redundant API calls
    # List existing match files in the season's directory
    season_prefix = f'bronze-raw/statsbomb/seasons/{compSeasonId}/player-stats/'
    existing_files = list_s3_files('traits-app', season_prefix)
    existing_match_ids = [int(file['Key'].split('/')[-1].replace('.json', '')) for file in existing_files if file['Key'].split('/')[-1].replace('.json', '').isdigit()]
    
    print(f"Existing matches: {len(existing_match_ids)}")
    
    if not existing_match_ids:  # If no files were found, all eligible matches are considered missing
        matches_to_update = logged_matches
    else:
        matches_to_update = [match for match in logged_matches if match['match_id'] not in existing_match_ids]
    
    print(f"Matches to update: {len(matches_to_update)}")
    
    # Retrieve match data from valid matches and write to S3 partition on league/season
    s3 = S3FileSystem()
    matchstats_endpoint = "https://data.statsbombservices.com/api/v4/matches/{match_id}/player-stats"
    lineups_endpoint = "https://data.statsbomb.com/api/v4/lineups/{match_id}"
    events_endpoint = "https://data.statsbomb.com/api/v8/events/{match_id}"
    
    # matches_ls = []
    # errors_ls = []
    # for match in matches_to_update:
    #     try:
    #         match_id = match['match_id']
    #         matches_ls.append(match_id) # Log match ids
    #         comp_id = str(match['competition']['competition_id'])
    #         season_id = str(match['season']['season_id'])
    #         comp_season_id = compSeasonId
    #         # Write match player stats json to S3
    #         stats_data = requests.request("GET", matchstats_endpoint.format(match_id = match_id), headers=headers).json()
    #         s3_path = 's3://traits-app/bronze-raw/statsbomb/seasons/{}/players-stats/{}.json'.format(comp_season_id, match_id)
    #         with s3.open(s3_path, 'w', encoding='utf8') as file:
    #             json.dump(stats_data, file, ensure_ascii=False)
    #         # Write lineups json to s3_path for the match
    #         lineups_data = requests.request("GET", lineups_endpoint.format(match_id = match_id), headers=headers).json()
    #         s3_path = 's3://traits-app/bronze-raw/statsbomb/seasons/{}/lineups/{}.json'.format(comp_season_id, match_id)
    #         with s3.open(s3_path, 'w', encoding='utf8') as file:
    #             json.dump(lineups_data, file, ensure_ascii=False)
    #         # OPTIONAL: uncomment for events
    #         # # Write events json to s3_path for the match
    #         # events_data = requests.request("GET", events_endpoint.format(match_id = match_id), headers=headers).json()
    #         # s3_path = 's3://traits-app/bronze-raw/statsbomb/seasons/{}/events/{}.json'.format(comp_season_id, match_id)
    #         # with s3.open(s3_path, 'w', encoding='utf8') as file:
    #         #     json.dump(events_data, file, ensure_ascii=False)
    #     except Exception as e:
    #         print(e)
    #         errors_ls.append(match['match_id'])
    
    # List of matches to update
    errors_ls = []
    
    # Call parallelized requests
    print(f"Parsing {len(matches_to_update)} matches for season {compSeasonId}.")
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit tasks to the executor for parallel processing
        future_to_match = {executor.submit(fetch_and_save_match_data, match, compSeasonId, headers): match for match in matches_to_update}
        
        for future in concurrent.futures.as_completed(future_to_match):
            match_id, result = future.result()
            if result == 'error':
                errors_ls.append(match_id)
                
    error_count = len(errors_ls)
    total_matches = len(matches_to_update)
    error_rate = (error_count / total_matches) * 100 if total_matches > 0 else 0

    print(f"Errors for season {compSeasonId}: {error_count} / {total_matches} ({len(logged_matches)})")
    if error_rate > 20:
        print(f"Warning: High error rate ({error_rate:.2f}%) for season {seasonId}")
        
    return compSeasonId
    
### MAIN LOOP THROUGH SEASONS ###

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

for season_tup in all_seasons_tup:
    
    print(f"Updating competition {season_tup[1]} ({season_tup[0]})...")
    print(f"Updating season {season_tup[3]} ({season_tup[2]})...")
    
    start = time.time()
    processed_season_id = process_season(season_tup[0], season_tup[2])
    
    print(f"Season {processed_season_id} parsed in {round((time.time() - start)/60,2)} mins")