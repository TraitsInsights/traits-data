import pandas as pd
import numpy as np
import sys
import json
import datetime as dt
import os
import re
from collections import Counter
import pyarrow.parquet as pq
import pyarrow as pa
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions

# retrieve client id passed from STEP FUNCTION map as parameter
print(sys.argv)
try:
    args = getResolvedOptions(sys.argv, ['client_id'])
    client_id = args['client_id']
except:
    args = getResolvedOptions(sys.argv, ['default_client_id'])
    client_id = args['default_client_id']
print(f"Client ID: {client_id}")

# load position mapping dictionary
s3 = S3FileSystem()
try:
    with s3.open('s3://traits-app/settings/positions_map.json', 'r', encoding='utf-8') as f:
        pos_key = json.load(f)['STATSBOMB']
    pos_key = {k: oldk for oldk, oldv in pos_key.items() for k in oldv}
except FileNotFoundError:
    print("positions_map.json file not found")
except ValueError:
    print("positions_map.json file is not in the expected format")

# Create age column
def age(birthdate, year):
    today = dt.datetime.today()
    try:
        birthdate_obj = dt.datetime.strptime(birthdate, '%Y-%m-%d')
        if today.year == year:
            age = today.year - birthdate_obj.year
            if today.month < birthdate_obj.month or (today.month == birthdate_obj.month and today.day < birthdate_obj.day):
                age -= 1
        else:
            age = int(year) - birthdate_obj.year
        return age
    except:
        return None

def update_parquet(df, s3_url, reset=True):
    s3 = S3FileSystem()

    def reconcile_schema(target_df, original_schema):
        """Reconciles the schema of a dataframe to match the provided schema"""
        # Drop columns not in the original schema
        for col in target_df.columns:
            if col not in original_schema:
                target_df = target_df.drop(columns=col)
        
        # Add missing columns from the original schema with NaNs
        for col in original_schema:
            if col not in target_df.columns:
                target_df[col] = None
        
        # Reorder columns to match original schema
        target_df = target_df[original_schema]
        return target_df
        
    if reset and s3.exists(s3_url):
        print(f"Reset is enabled, deleting existing directory: {s3_url}")
        s3.delete(s3_url, recursive=True)  # Delete the directory and its contents
        print(f"Directory {s3_url} deleted.")

    if not s3.exists(s3_url):
        print("Directory does not exist, creating and writing parquet file")
        df.to_parquet(s3_url, partition_cols=['latterYear'], engine='pyarrow', index=False)
    else:
        print("Directory exists, updating parquet files")
        
        # TODO: won't write to new paritions
        df = df.set_index('playerTeamSeasonCompetition') # for updates, set new data index to PTSC
        for year in sorted(df['latterYear'].unique()):
            partition_path = f"{s3_url}/latterYear={year}"
            print(partition_path)
            partition_files = s3.glob(f"{partition_path}/*.parquet")
            print(partition_files)
            print(f"Partition found for latterYear={year}, updating")
            
            for n, s3_parquet_path in enumerate(partition_files):
                partition_df = pd.read_parquet("s3://" + s3_parquet_path)
                # Read the schema of the original parquet file
                original_schema = partition_df.columns.tolist() #pd.read_parquet("s3://" + s3_url, engine='pyarrow').columns.tolist() #, nrows=0
                partition_df = partition_df.set_index('playerTeamSeasonCompetition') # for updates, set existing data index to PTSC
                matching_indexes = partition_df.index.isin(df.index)
                print(f"Players overwritten: {matching_indexes.sum()}")
                partition_df.update(df[df['latterYear'] == year], overwrite=True)
                
                # Add new rows
                non_matching_indexes = ~df[df['latterYear'] == year].index.isin(partition_df.index)
                new_rows = df[df['latterYear'] == year][non_matching_indexes]
                partition_df = pd.concat([partition_df, new_rows])

                # Reset index to numeric index
                partition_df = partition_df.reset_index()
                
                # Reconcile the schema to match the original parquet file's schema
                partition_df = reconcile_schema(partition_df, original_schema)
                
                print(f"New players added {new_rows.shape}")
                
                partition_df.to_parquet("s3://"+s3_parquet_path, engine='pyarrow') 

# select most recent json file (contains all updated leagues since last push, assuming this script is synced) 
files = s3.ls('traits-app/bronze-raw/statsbomb/{}/'.format(client_id))
print(files)
files = [re.findall('\d+', f)[1] for f in files if len(re.findall('\d+', f))>1]
print(files)
files.sort(key=lambda date: dt.datetime.strptime(date, '%d%m%Y'))
latest_file = files[-1]
print("Latest file to parse for client ID: ", client_id, "is ", latest_file)

# specify directories
EXISTING_PARQUET = 's3://traits-app/silver-cleaned/{0}/parquet'.format(client_id)
LATEST_JSON = 's3://traits-app/bronze-raw/statsbomb/{0}/'.format(client_id) + latest_file + '.json'

# TODO: delete existing parquet if reset_data == 1

# load dataframe to be appended
try:
    new_df = pd.read_json(LATEST_JSON, encoding='utf-8')
    print(f"Latest file has a shape of {new_df.shape} with {new_df['player_id'].nunique()} unique player (StatsBomb ids).")
    string_cols = ['account_id', 'player_id', 'player_name', 'player_known_name', 'team_id', 'team_name', 'competition_id', 'competition_name', 'season_id', 'season_name', 'country_id', 'birth_date', 'player_female', 'player_first_name', 'player_last_name', 'primary_position', 'secondary_position', 'player_season_most_recent_match']
    dtypes = {col: str if col in string_cols else float for col in new_df.columns}
    # Convert columns to specified data types
    new_df = new_df.astype(dtypes)
except FileNotFoundError:
    print("specified json file not found")
except pd.errors.EmptyDataError:
    print("specified json file is empty")
    
# load player data
player_path = f's3://traits-app/deployments/{client_id}/players.csv'
player_df = pd.read_csv(player_path).drop_duplicates(subset=['offline_player_id'], keep='last')
player_df['offline_player_id'] = player_df['offline_player_id'].astype('str')
print(f"Client has {player_df.shape[0]} unique player ids in database.")

# merge player data
new_df = new_df.merge(player_df[['offline_player_id', 'player_height', 'player_weight', 'player_preferred_foot', 'country_of_birth_name']], how = 'left', left_on = 'player_id', right_on = 'offline_player_id')

# merge competition display names

with s3.open('s3://traits-app/settings/statsbomb_competitions.json', 'r', encoding='utf-8') as f:
    comps_data = json.load(f)

comps_dict = {entry['competition_id']: entry for entry in comps_data}
display_name_dict = {str(comp_id): details['display_name'] for comp_id, details in comps_dict.items()}
new_df['competition_name'] = new_df['competition_id'].map(display_name_dict).fillna(new_df['competition_name'])

print(new_df.groupby('competition_name').count()['competition_id'])
print(display_name_dict)

# append columns
new_df['playerDOB'] = new_df['player_name'] + ' ' + new_df['birth_date']
new_df['latterYear'] = new_df['season_name'].map(lambda x: str(x).split("/")[-1]) # a unique identifer for the year in which the season ends
new_df['age'] = new_df.apply(lambda row: age(row['birth_date'], row['latterYear']), axis=1)  # new_df['birth_date'].map(lambda x: age(str(x)))
new_df['teamSeason'] = new_df['team_name'] + ' ' + new_df['season_name']
new_df['defaultName'] = new_df['player_known_name'].where(~new_df['player_known_name'].replace('None', np.nan).fillna(value=np.nan).isna(), new_df['player_name'])
new_df['playerTeamSeason'] = new_df['defaultName'] + ' ' + new_df['teamSeason'] # changed from player_name to defaultName
new_df['playerTeamSeasonCompetition'] = new_df['playerTeamSeason'] + ' ' + new_df['competition_name']
new_df['posAbbr'] = new_df['primary_position'].map(pos_key)
new_df["nationality"] = new_df["country_of_birth_name"].astype(str)

# rename columns
new_df = new_df.rename(columns={"defaultName":"playerName",
                                    "player_season_appearances":"appearances",
                                   "competition_name":"competitionName",
                                   "team_name":"teamName",
                                   "season_name":"seasonName",
                                    "player_first_name":"firstName",
                                    "player_last_name":"lastName",
                                    "primary_position":"detailedPosition",
                                    "birth_date":"DOB",
                                    "player_preferred_foot":"preferredFoot",
                                    "player_weight":"playerWeight",
                                    "player_height":"playerHeight",
                                    #"player_known_name":"knownName"
                                    #"player_season_average_minutes":"minutes"
                                   })
                                   
# remove player_season prefix from columns
new_df.columns = [c.replace('player_season_', '') for c in new_df.columns.values]

# read in existing file and rewrite with updates
# print("EG SQUADS:", new_df.groupby(['teamName', 'seasonName']).count()[['playerTeamSeasonCompetition']].sample(10)) ###

# assert no playerTeamSeason duplicates
# TODO: add team remappings for cup teams, and check pTS here
# NOTE: where cup competitions are played, this should be a separate id and pTS string
print(f"{new_df['playerTeamSeason'].nunique()} unique player-team profiles in update file.")
print(f"{new_df['playerTeamSeasonCompetition'].nunique()} unique player-team-competition profiles in update file.")

dups_pts = new_df.loc[new_df.duplicated(subset=['playerTeamSeason'], keep='last'),:]
print(f"{dups_pts.shape} duplicates on playerTeamSeason")

dups_ptsc = new_df.loc[new_df.duplicated(subset=['playerTeamSeasonCompetition'], keep='last'),:]
print(f"{dups_ptsc.shape} duplicated on playerTeamSeasonCompetition")

### DEBUG DUPLICATES
if dups_pts.shape[0] > 0:
    #print(new_df[["playerTeamSeason", "playerTeamSeasonCompetition", "posAbbr", "playerDOB", "playerName"]].loc[new_df.duplicated(subset=['playerTeamSeason'], keep=False),:].head(20))
    new_df.loc[new_df.duplicated(subset=['playerTeamSeason'], keep=False),:].to_csv(f's3://traits-app/silver-cleaned/{client_id}/duplicates_PTS.csv')
    print("Duplicates on PTS written to file.")
    
if dups_ptsc.shape[0] > 0:
    print(new_df[["playerTeamSeason", "playerTeamSeasonCompetition", "posAbbr", "playerDOB", "playerName"]].loc[new_df.duplicated(subset=['playerTeamSeasonCompetition'], keep=False),:])
    new_df.loc[new_df.duplicated(subset=['playerTeamSeasonCompetition'], keep=False),:].to_csv(f's3://traits-app/silver-cleaned/{client_id}/duplicates_PTSC.csv')
    print("Duplicates on PTSC written to file.")
    
# TODO: should not need to drop duplicates here unless legitimately in the source data
# drop the profile with fewer games when champion have marked multiple positions
print("Defaulting to max appearances for duplicate playerTeamSeasonCompetition entries")
idx = new_df.groupby('playerTeamSeasonCompetition')['appearances'].idxmax()
new_df = new_df.loc[idx]
new_df = new_df.drop_duplicates(subset=['playerTeamSeasonCompetition'])

assert new_df.loc[new_df.duplicated(subset=['playerTeamSeasonCompetition'], keep='last'),:].shape[0] == 0

# update parquet file(s)
update_parquet(new_df, EXISTING_PARQUET)
new_df.to_csv(f's3://traits-app/silver-cleaned/{client_id}/cleaned.csv')

#if int(reset_data) == 0:
    # print(f"UPDATED DB FOR ALL LEAGUES AND CURRENT YEAR: {current_year}")
#else:
#    print("UPDATED DB FOR ALL LEAGUES AND SEASONS")

print("UPDATED DB WITH FILE: ", LATEST_JSON)
    
