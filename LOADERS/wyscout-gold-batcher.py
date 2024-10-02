import ast
import sys
import boto3
import pandas as pd
import requests
from time import sleep
from datetime import datetime
from awsglue.utils import getResolvedOptions
import json

# Initialize the AWS Boto3 client for Glue
glue_client = boto3.client('glue')

# Extract parameters from the job's execution context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'priority_group', 'seasons_list', 'min_year', 'max_year', 'default_client_id', 'basic'])
try:
    seasons_list = ast.literal_eval(args['seasons_list'])
    print(f"{len(seasons_list)} seasons passed as parameter.")
except:
    seasons_list = []

try:
    priority_group = int(args['priority_group'])
except:
    priority_group = 99
min_year = int(args['min_year'])
max_year = int(args['max_year'])
basic = args['basic']

try:
    client_id = str(args['client_id'])
except:
    client_id = str(args['default_client_id'])

# Load competitions from S3
if priority_group != 99:
    comps_df = pd.read_csv('s3://traits-app/settings/wyscout_competitions.csv')
    comps_df = comps_df[comps_df['priorityGroup'] == priority_group]
    comps_ls = comps_df['wyId'].tolist()
    print(f"{len(comps_ls)} competitions in designated priority group.")

# Define headers for HTTP requests
headers = {
    'Authorization': basic
}

# Function to fetch season data from an API
def fetch_season_data(wyId):
    _season_endpoint = f"https://apirest.wyscout.com/v3/competitions/{wyId}/seasons"
    try:
        response = requests.get(_season_endpoint, headers=headers)
        sleep(0.1)  # Sleep to manage API rate limits
        if response.status_code == 200:
            data = response.json()
            if 'seasons' in data and data['seasons']:
                print(f"Retrieved seasons for competition: {wyId}")
                return data['seasons']
            else:
                print(f"No seasons available for competitionId: {wyId}")
                return None
        else:
            print(f"{response.status_code}: Failed to fetch seasons for competitionId: {wyId}")
            return None
    except Exception as e:
        print(f"Exception while fetching seasons for competitionId: {wyId}: {str(e)}")
        return None

# Fetch and filter season data
seasons_lol = []

def batch_list(input_list, n):
    return [input_list[i:i + n] for i in range(0, len(input_list), n)]

if seasons_list:
    seasons_lol = batch_list(seasons_list, 50)
else:
    for competitionId in comps_ls:
        _seasons = fetch_season_data(competitionId)
        if _seasons:
            seasons_df = pd.json_normalize(_seasons)
            seasons_df['startYear'] = pd.to_datetime(seasons_df['season.startDate']).dt.year
            valid_seasons = seasons_df[(seasons_df['startYear'] >= int(min_year)) & (seasons_df['startYear'] <= int(max_year)) & (pd.to_datetime(seasons_df['season.startDate']) < datetime.now())]
            print(f"{len(valid_seasons)} valid seasons for competition {competitionId}, starting {seasons_df['startYear'].unique()}")
            seasons_lol.append(list(valid_seasons['seasonId']))
            
    assert len(comps_ls) == len(seasons_lol)

print(f"Calling {len(seasons_lol)} glue jobs.")
print(f"Total seasons in batch: {sum(len(sublist) for sublist in seasons_lol)}")

for job_no, seasons_batch in enumerate(seasons_lol):
    if job_no % 5 == 0:
        sleep(60)
        print("Resting...")
    if seasons_batch:
        response = glue_client.start_job_run(
            JobName='GOLD-data-loader',
            Arguments={
                '--seasons_to_process': json.dumps(seasons_batch),
                '--client_id': str(client_id),
                '--data_provider': 'wyscout'
            }
        )
        print(f'Glue job started for seasons {seasons_batch} with response: {response}')
    else:
        print(f"Skipping {seasons_batch}")