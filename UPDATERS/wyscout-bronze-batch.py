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

### CONFIG ###
# Retrieve client id passed from STEP FUNCTION map as parameter
try:
    args = getResolvedOptions(sys.argv, ['WYSCOUT_AUTH', 'seasons_to_update'])
except:
    args = getResolvedOptions(sys.argv, ['WYSCOUT_AUTH', 'seasons_to_update'])
    
# define Wyscout auth credentials
auth = args['WYSCOUT_AUTH']

# Expects a list of seasons that are to be checked for updates
seasons_to_update = ast.literal_eval(args['seasons_to_update'])

payload = {}
headers = {
  'Authorization': str(auth)
}

s3 = S3FileSystem() #Initiate file manager
b3 = boto3.client('s3')

# Define Wyscout API endpoint wrappers
def fetch_and_save_match_data(seasonId, matchId):
    max_retries = 3
    
    # endpoints
    player_stats_endpoint = f"https://apirest.wyscout.com/v3/matches/{matchId}/advancedstats/players?details=player" #&fetch=round
    match_stats_endpoint = f"https://apirest.wyscout.com/v3/matches/{matchId}/advancedstats"
    player_s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{seasonId}/player-stats/{matchId}.json'
    match_s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{seasonId}/match-stats/{matchId}.json'
    
    for attempt in range(max_retries):
        try:
            player_stats_response = requests.get(player_stats_endpoint, headers=headers)
            match_stats_response = requests.get(match_stats_endpoint, headers=headers)

            if player_stats_response.status_code == 200 and match_stats_response.status_code == 200:
                player_stats_data = player_stats_response.json()
                match_stats_data = match_stats_response.json()

                with s3.open(player_s3_path, 'w', encoding='utf8') as file:
                    json.dump(player_stats_data, file, ensure_ascii=False)

                with s3.open(match_s3_path, 'w', encoding='utf8') as file:
                    json.dump(match_stats_data, file, ensure_ascii=False)

                return seasonId, matchId, 'rewrite'

            else:
                player_error = player_stats_response.json().get("error", {})
                match_error = match_stats_response.json().get('error', {})
                if ("No statistical data available" in player_error.get("message", "")) or ("No statistical data available" in match_error.get("message", "")):
                    if s3.exists(player_s3_path):
                        s3.rm(player_s3_path)
                    if s3.exists(match_s3_path):
                        s3.rm(match_s3_path)
                    return seasonId, matchId, 'delete'
                else:
                    print(f"({attempt + 1}/{max_retries}) Failed to fetch data for seasonId {seasonId}, matchId {matchId}: Player stats {player_error}, Match stats {match_error}")
                    time.sleep(1.5)
                    continue
        except requests.exceptions.RequestException as e:
            print(f"Retrying ({attempt + 1}/{max_retries}) for seasonId {seasonId}, matchId {matchId} due to RequestException: {str(e)}")
            time.sleep(1.5)
    
    print(f"Max retries exceeded for seasonId {seasonId}, matchId {matchId}")
    return seasonId, matchId, 'error'  # Ensure that the function returns an error indicator
    
def convert_date(date_str):
    return dt.datetime.strptime(date_str, '%Y-%m-%d')

def save_fixtures_to_s3(data, wyId):
    s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{wyId}/fixtures.json'
    try:
        if data:
            with s3.open(s3_path, 'w', encoding='utf8') as file:
                json.dump(data, file, ensure_ascii=False)
        else:
            print(f"No fixtures data to save for season {wyId}")
    except Exception as e:
        print(f"Failed to save fixtures data for {wyId}: {str(e)}")
     
def save_teams_to_s3(data, season_id):
    s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{season_id}/teams.json'
    try:
        if data:
            with s3.open(s3_path, 'w', encoding='utf8') as file:
                json.dump(data, file, ensure_ascii=False)
        else:
            print(f"No team data to save for season {wyId}")
    except Exception as e:
        print(f"Failed to save teams data for {wyId}: {str(e)}")
        
def fetch_teams(season_id, max_retries=3):
    # TODO: implement retries
    _teams_endpoint = f"https://apirest.wyscout.com/v3/seasons/{season_id}/teams?fetch=season"
    s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{season_id}/teams.json'
    try:
        if s3.exists(s3_path):
            print(f"Teams data for season {season_id} already exists in S3 at {s3_path}")
            return None, None
        response = requests.get(_teams_endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'teams' in data and data['teams']:
                save_teams_to_s3(data, season_id)
                print(f"Retrieved and saved teams for season: {season_id}")
                return season_id, data
            else:
                print(f"No teams data available for seasonId: {season_id}")
                return None, None
        else:
            print(f"{response.status_code}: Failed to fetch teams for seasonId: {season_id}")
            return None, None
    except Exception as e:
        print(f"Exception while fetching teams for seasonId: {season_id}: {str(e)}")
        return None, None
        
def fetch_fixtures(season_id, update=False, max_retries=3):

    s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{season_id}/fixtures.json'
    
    if update==False:
        # Check if the fixtures file already exists in S3
        try:
            if s3.exists(s3_path):
                print(f"Fixtures data for season {season_id} already exists in S3 at {s3_path}")
                with s3.open(s3_path, 'r', encoding='utf8') as file:
                    data = json.load(file)
                return data['matches'] if 'matches' in data and data['matches'] else None
        except Exception as e:
            print(f"Error checking or reading fixtures data from S3 for season {season_id}: {str(e)}")
    
    # If file does not exist, fetch data from API
    _endpoint = f"https://apirest.wyscout.com/v3/seasons/{season_id}/fixtures?details=matches"
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(_endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if 'matches' in data and data['matches']:
                    print(f"Retrieved fixtures for season: {season_id}")
                    # Save the data to S3
                    save_fixtures_to_s3(data, season_id)
                    return data['matches']
                else:
                    print(f"No fixtures data available for seasonId: {season_id}")
                    return None
            else:
                print(f"Failed to fetch data from API for seasonId: {season_id}, status code: {response.status_code}")
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff
        except requests.exceptions.RequestException as e:
            print(f"RequestException while fetching fixtures for seasonId: {season_id}: {str(e)}")
            retries += 1
            time.sleep(2 ** retries)  # Exponential backoff
    
    print(f"Max retries exceeded while fetching fixtures for seasonId: {season_id}")
    return None
    
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
    
def process_season(seasonId):
    
    """Fetches player and team match stats based on a fixtures json file and saves individual resultss to s3 in a multi-threaded process."""
    
    print(f"Processing season: {seasonId}")
    
    # Check teams data is written
    _, teams = fetch_teams(season_id=seasonId)
    
    # Fetch and write the latest fixtures
    matches = fetch_fixtures(season_id=seasonId, update=True)
        
    # List existing match files in the season's directory
    season_prefix = f'bronze-raw/wyscout/seasons/{seasonId}/player-stats/'
    logged_matches = [match for match in matches if match['match']['hasDataAvailable'] and match['match']['status'] == 'Played']
    existing_files = list_s3_files('traits-app', season_prefix)
    existing_match_ids = [int(file['Key'].split('/')[-1].replace('.json', '')) for file in existing_files if file['Key'].split('/')[-1].replace('.json', '').isdigit()]
    
    if not existing_match_ids:  # If no files were found, all eligible matches are considered missing
        matches_ls = [match['matchId'] for match in logged_matches]
    else:
        matches_ls = [match['matchId'] for match in logged_matches if match['matchId'] not in existing_match_ids]

    errors_ls = []

    # Call parallelized requests
    print(f"Parsing {len(matches_ls)} matches for season {seasonId}.")
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_matches = {executor.submit(fetch_and_save_match_data, seasonId, matchId): matchId for matchId in matches_ls}
        for future in concurrent.futures.as_completed(future_matches):
            season, match_id, result = future.result()
            if result == 'error':
                errors_ls.append((season, match_id))
                
    error_count = len(errors_ls)
    total_matches = len(matches_ls)
    error_rate = (error_count / total_matches) * 100 if total_matches > 0 else 0

    print(f"Errors for season {seasonId}: {error_count} / {total_matches} ({len(logged_matches)})")
    if error_rate > 20:
        print(f"Warning: High error rate ({error_rate:.2f}%) for season {seasonId}")

    return seasonId
    
### MAIN LOOP THROUGH SEASONS ###

for seasonId in seasons_to_update:
    
    print(f"Updating season {seasonId}...")
    
    start = time.time()
    processed_season_id = process_season(seasonId)
    
    print(f"Season {seasonId} parsed in {round((time.time() - start)/60,2)} mins")