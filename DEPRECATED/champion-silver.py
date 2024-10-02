import ast
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
import boto3
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions

# retrieve client id passed from STEP FUNCTION map as parameter
print(sys.argv)
# try:
#     args = getResolvedOptions(sys.argv, ['client_id', 'current_year'])
#     client_id = args['client_id']
# except:
args = getResolvedOptions(sys.argv, ['default_client_id', 'reset', 'current_seasons'])
client_id = args['default_client_id']
reset_data = args['reset']
current_seasons = ast.literal_eval(args['current_seasons'])
print(f"Client ID: {client_id}")

# load position mapping dictionary
s3 = S3FileSystem()
try:
    with s3.open('s3://traits-app/settings/positions_map.json', 'r', encoding='utf-8') as f:
        pos_key = json.load(f)['CHAMPION']
    pos_key = {k: oldk for oldk, oldv in pos_key.items() for k in oldv}
except FileNotFoundError:
    print("positions_map.json file not found")
except ValueError:
    print("positions_map.json file is not in the expected format")

def load_csv_files(directory, reset):
    s3 = S3FileSystem()
    files = s3.ls(directory)
    
    dataframes = []
    for filename in files:
        if filename.endswith('.csv'):
            file_path = 's3://' + filename
            df = pd.read_csv(file_path, encoding='utf-8')
            cols = df.columns.values
            league = filename.split("/")[-1].split("_")[-2]
            season = filename.split("_")[-1][0:-4]
            print(league, "-", season)
            if int(reset) == 0:
                # if not resetting data, only update current seasons as defined by current_year parameter
                if any(year in filename for year in current_seasons):
                    df['seasonName'] = season
                    df['competitionName'] = league
                    if "Player" in cols:
                        df['Player ID'] = df['Player'] + ' ' + season
                    else:
                        df['Player ID'] = df['Player Name'] + ' ' + season
                    dataframes.append(df)
            elif int(reset) == 1:
                df['seasonName'] = season
                df['competitionName'] = league
                if "Player" in cols:
                    df['Player ID'] = df['Player'] + ' ' + season
                else:
                    df['Player ID'] = df['Player Name'] + ' ' + season
                dataframes.append(df)
            else:
                print("Reset parameter could not be determined.")
    if dataframes:
        merged_df = pd.concat(dataframes, ignore_index=True)
        return merged_df
    else:
        return None
     
def update_parquet(df, s3_url):
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

    if not s3.exists(s3_url):
        print("Directory does not exist, creating and writing parquet file")
        df.to_parquet(s3_url, partition_cols=['latterYear'], engine='pyarrow', index=False)
    else:
        print("Directory exists, updating parquet files")
    
        df = df.set_index('playerTeamSeasonCompetition')
        for year in df['latterYear'].unique():
            partition_path = f"{s3_url}/latterYear={year}"
            partition_files = s3.glob(f"{partition_path}/*.parquet")
            print(f"Partition found for latterYear={year}, updating")
            
            for n, s3_parquet_path in enumerate(partition_files):
                partition_df = pd.read_parquet("s3://" + s3_parquet_path)
                original_schema = partition_df.columns.tolist()
                partition_df = partition_df.set_index('playerTeamSeasonCompetition')
                matching_indexes = partition_df.index.isin(df.index)
                print(f"Players overwritten: {matching_indexes.sum()}")
                partition_df.update(df[df['latterYear'] == year], overwrite=True)

                # Add new rows
                non_matching_indexes = ~df[df['latterYear'] == year].index.isin(partition_df.index)
                new_rows = df[df['latterYear'] == year][non_matching_indexes]
                partition_df = pd.concat([partition_df, new_rows])

                # Reconcile the schema to match the original parquet file's schema
                partition_df = reconcile_schema(partition_df, original_schema)
                
                print(f"New players added {new_rows.shape}")
                partition_df.to_parquet("s3://"+s3_parquet_path, engine='pyarrow') #.reset_index()
                
# def update_parquet(df, s3_url):
    
#     s3 = S3FileSystem()

#     if not s3.exists(s3_url):
#         print("Directory does not exist, creating and writing parquet file")
#         df.to_parquet(s3_url, partition_cols=['latterYear'], engine='pyarrow', index=False)
#     else:
#         print("Directory exists, updating parquet files")
#         df = df.set_index('playerTeamSeasonCompetition')
#         for year in df['latterYear'].unique():
#             partition_path = f"{s3_url}/latterYear={year}"
#             partition_files = s3.glob(f"{partition_path}/*.parquet")
#             print(f"Partition found for latterYear={year}, updating")
#             for n, s3_parquet_path in enumerate(partition_files):
#                 partition_df = pd.read_parquet("s3://"+s3_parquet_path)
#                 # partition_df.drop_duplicates(subset=['playerTeamSeasonCompetition'], keep = 'last', inplace=True)
#                 # TODO: print summary of duplicates
#                 partition_df = partition_df.set_index('playerTeamSeasonCompetition')
#                 matching_indexes = partition_df.index.isin(df.index)
#                 print(f"Players overwritten: {matching_indexes.sum()}")
#                 partition_df.update(df[df['latterYear'] == year], overwrite=True)
#                 # TODO: check if this is adding newly eligible players 
#                 #if n == len(partition_files)-1:
#                 #    partition_df = pd.concat([partition_df, df[df['latterYear'] == year][~df['playerTeamSeasonCompetition'].isin(partition_df['playerTeamSeasonCompetition'])]])
                
#                 # Add new rows
#                 non_matching_indexes = ~df[df['latterYear'] == year].index.isin(partition_df.index)
#                 new_rows = df[df['latterYear'] == year][non_matching_indexes]
#                 partition_df = pd.concat([partition_df, new_rows])
#                 print(f"New players added {new_rows.shape}")
                
#                 partition_df.reset_index().to_parquet("s3://"+s3_parquet_path, engine='pyarrow')
                
# define dictionary for remapping duplicate team names (there is a high chance of 
# some duplicates existing given non-full names and duplicate teams across leagues)
team_renames = {
'AFLW':' W',
'Coates League Boys': ' Boys',
'Coates League Girls': ' Girls',
'SANFL': ' SA',
'SANFL Reserves': ' SA Res',
'SANFL U18': ' SA U18',
'SANFLW': '(W) SA',
'U18 Boys Championships':' Champs B',
'U18 Girls Championships':' Champs G',
'VFL':' VFL',
'VFLW':' VFLW',
'WAFL':' WAFL',
'WAFL Reserves':' WAFL Res',
'WAFL Colts':' WAFL',
'WAFLW':' WAFL'
}

# specify directories
EXISTING_PARQUET = 's3://traits-app/silver-cleaned/3/parquet'
# Specify the directory to parse
directory = 'traits-app/bronze-raw/champion/stats'
# Specify the prefixes to search for
#prefixes = ['AFL']

# load dataframe to be appended
#try:
new_df = load_csv_files(directory, reset_data)
new_df = new_df.rename(columns={"Player Name":"Player"})
new_df.columns = [c.strip().replace("\xa0", " ") for c in new_df.columns.values]
new_df.columns = [c.replace(" - ", " ").replace("-"," ").replace("  "," ") for c in new_df.columns.values]
print(new_df.shape)
# except FileNotFoundError:
#     print("error reading csv files")
    
# load in players table
# try:
players = load_csv_files('s3://traits-app/bronze-raw/champion/players/',reset_data)
print("Players loaded.")

# get age based on oldest age during latter season year

def age(birthdate_str, current_date_str=None):
    try:
        birthdate = dt.datetime.strptime(birthdate_str, '%d/%m/%Y')

        # Use the provided current_date_str or get the current date if not provided
        if current_date_str is None:
            current_date = dt.datetime.now()
        else:
            current_date = dt.datetime.strptime(current_date_str, '%d/%m/%Y')
            
        if int(current_date.year) >= int(dt.datetime.now().year):

            # calculate current age based on date for current season
            
            age = current_date.year - birthdate.year
    
            if (current_date.month, current_date.day) < (birthdate.month, birthdate.day):
                age -= 1
        else:
            age = current_date.year - birthdate.year
        
        return age

    except Exception as e:
        # print(f"Error calculating age: {e}")
        return 0

# except FileNotFoundError:
#     print("players csv file(s) not found")

# drop rows with nan as player
new_df = new_df.replace('nan', np.nan)
print(new_df.columns.values)
#print(f"Count of NA players: {new_df.loc[new_df['Player'].isna()].shape[0]}")
new_df.dropna(subset=['Player'], inplace=True)

# merge player data
# not incl. 'Full Name', 'Club', 'Jumper No.', 'Rookie', 'Goals', 'Matches'
print("Shape before merge:", new_df.shape)
new_df = new_df.merge(players[["Player ID", 'DOB', 'Height', 'Weight']].drop_duplicates(subset=['Player ID'], keep='last'), left_on = "Player ID", right_on = "Player ID", how = "left") #'Age',
print("Player data merged.")

# append columns
# rename cols
new_df = new_df.rename(columns={"Mt":"appearances",
                                   "League":"competitionName",
                                    "Position":"detailedPosition"
                                   })
                                 
# rename teams to maintain uniqueness across leagues
def add_suffix(competition, team):
    return team + competition.map(lambda x: team_renames.get(x, ""))
    
def extract_outermost_brackets(text):
    try:
        match = re.search(r'\((.*)\)', text)
        if match:
            return match.group(1)
        else:
            return None
    except Exception:
        print(text)
        return str(text)
    
new_df['teamAbbr'] = new_df['Player'].astype('str').apply(extract_outermost_brackets)
print("...teamAbbr")
new_df["teamName"] = new_df["teamAbbr"].apply(lambda x: str(x).split(",")[0].strip())
new_df["teamName"] = add_suffix(new_df["competitionName"], new_df["teamName"])
print(new_df["teamName"].unique())
print("...teamName")

# this regex requires the basic strudicture of Initial.Surname (TEAM)
# it does allow for 
    # a variety of initials including Le.Young vs La.Young
    # a surname including a hyphen, apostrophe or a space (O'Connor, De Koenig, Ugle-Hagan)
    # a single or double listed team eg (WBFC) or (WBFC, SSFC)
#player_regex = r'^[a-zA-Z]+\.[A-Za-z\- \']+ \([a-zA-Z, ]+\)$'
#if not new_df['Player'].str.match(player_regex).all():
#    raise ValueError("Player column contains strings that do not match the format 'Initial.Surname (TEAM)'")

print("Creating columns...")
new_df['playerName'] = new_df['Player'].apply(lambda x: str(x).split("(")[0].strip())
print("...playerName")
#new_df["firstName"] = new_df['Full Name'].apply(lambda x: x.split(" ")[0].strip()) # will have some errors for names with spaces
#new_df["lastName"] = new_df['playerName'].apply(lambda x: x.split(".")[1].strip())
new_df['playerDOB'] =  new_df['playerName'] + ' ' + new_df['DOB']
print("...playerDOB")
new_df['teamSeason'] = new_df['teamAbbr'] + ' ' + new_df['seasonName'].astype('str')
print("...teamSeason")
new_df['playerTeamSeason'] = new_df['playerName'] + ' ' + new_df['teamName'] + ' ' + new_df['seasonName']
print("...playerTeamSeason")
new_df['playerTeamSeasonCompetition'] = new_df['playerTeamSeason'] + ' ' + new_df['competitionName']
print("...playerTeamSeasonCompetition")
# latterYear for AFL is the numeric characters from the given season
new_df['latterYear'] = new_df['seasonName'].str.findall(r'\d+').str.join('')
print("...latterYear")
new_df['posAbbr'] = new_df['detailedPosition'].map(pos_key)
print("...posAbbr")
new_df['nationality'] = "N/A"
# no minute provided by champion
new_df["minutes"] = 0
new_df['age'] = new_df.apply(lambda row: age(row['DOB'], "31/12/" + row['latterYear']), axis=1)
print("...age")

# GENERATE DERIVED STATS
new_df = new_df.dropna(subset=["Disposal"])
new_df = new_df.loc[new_df["Disposal"]!=0]
new_df["Disposal"] = new_df["Disposal"].astype(float)
new_df["Mark Disposal Pct"] = new_df["Mark"] / new_df["Disposal"]
new_df["Contested Mark Disposal Pct"] = new_df["Contested Mark"] / new_df["Disposal"]
new_df["Handball Receive Disposal Pct"] = new_df["Handball Receive"] / new_df["Disposal"]
new_df["I50 Disposal Pct"] = new_df["Inside 50"] / new_df["Disposal"]
new_df["R50 Disposal Pct"] =  new_df["Rebound 50"] / new_df["Disposal"]
new_df["Shot At Goal Disposal Pct"] =  new_df["Shot At Goal"] / new_df["Disposal"]
new_df["Intercept Disposal Pct"] =  new_df["Intercept"] / new_df["Disposal"]

print("...derived stats.")

# enforce schema
string_cols = ['competitionName', 'Player', 'Player ID', 'Position', "detailedPosition", "seasonName",
"DOB", "age", "playerName", "teamAbbr", "teamName", "playerDOB", "teamSeason", "playerTeamSeason", "playerTeamSeasonCompetition", "posAbbr", "nationality", "latterYear"]
dtypes = {col: str if col in string_cols else float for col in new_df.columns}
# Convert columns to specified data types
new_df = new_df.astype(dtypes)

# deal with the few players Champion has assigned multiple positions to within a single season
# approximately 60 players (0.0015%)
# def append_posAbbr_to_name(df):

#     # Find names that have multiple distinct 'posAbbr' values
#     names_to_update = df.groupby("playerTeamSeason")["posAbbr"].nunique()
#     names_to_update = names_to_update[names_to_update > 1].index.tolist()

#     # Update the 'name' column for these names
#     mask = df["name"].isin(names_to_update)
#     df.loc[mask, "name"] = df.loc[mask, "name"] + ' ' + df.loc[mask, "posAbbr"]

#     return df
    
# new_df = append_posAbbr_to_name(new_df)

# drop the profile with fewer games when champion have marked multiple positions
idx = new_df.groupby('playerTeamSeason')['appearances'].idxmax()
new_df = new_df.loc[idx]
    
###### DEBUGGING #################

print(f"{new_df[new_df['playerTeamSeasonCompetition'].duplicated(keep='first')]['playerTeamSeasonCompetition'].nunique()} duplicates on playerTeamSeasonCompetition to be dropped")

print(f"Number of playerTeamSeason: {len(new_df)}")
print(f"Number of unique playerTeamSeason: {new_df['playerTeamSeason'].nunique()}")

teams_in_leagues = new_df.groupby('teamName')['competitionName'].nunique()
teams_in_multiple_leagues = teams_in_leagues[teams_in_leagues > 1].index.tolist()

print(f"Overlapping teams (exist in more than one comp): {teams_in_multiple_leagues}")

print(f"Count of duplicates PTS: {new_df[new_df.duplicated(subset=['playerTeamSeason'], keep=False)].shape}")
print(f"Count of duplicates PTSC: {new_df[new_df.duplicated(subset=['playerTeamSeason', 'competitionName'], keep=False)].shape}")

print("Duplicate values (PTS)")
print(new_df.loc[new_df.duplicated(subset=['playerTeamSeason'], keep='last'), ['playerTeamSeason', 'playerTeamSeasonCompetition', 'competitionName']].sort_values(by='playerTeamSeason'))

# export duplicate PTSC
# new_df.loc[new_df.duplicated(subset=['playerTeamSeason'], keep=False),:].to_csv('s3://traits-app/silver-cleaned/3/duplicates_PTC##.csv')
# new_df.loc[new_df.duplicated(subset=['playerTeamSeasonCompetition'], keep=False),:].to_csv('s3://traits-app/silver-cleaned/3/duplicates_PTSC.csv')
    
############################################################################################

# assert no duplicates exist on pTS key
# new_df.drop_duplicates(subset=['playerTeamSeasonCompetition'], keep = 'last', inplace=True)
assert new_df.loc[new_df.duplicated(subset=['playerTeamSeason'], keep='last'),:].shape[0] == 0

# read in existing file and rewrite with updates
update_parquet(new_df, EXISTING_PARQUET)
new_df.to_csv('s3://traits-app/silver-cleaned/3/AFL.csv')

if int(reset_data) == 0:
    print(f"UPDATED DB FOR ALL LEAGUES AND CURRENT SEASONS: {current_seasons}")
    print(new_df.groupby(["competitionName", "seasonName"]).count()['playerTeamSeason'])
else:
    print("UPDATED DB FOR ALL LEAGUES AND SEASONS")