import ast
import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import s3fs
import sys
import time
from collections import Counter
from s3fs import S3FileSystem
from scipy.stats import zscore
from math import ceil
from awsglue.utils import getResolvedOptions

# define helper functions
def z_score(x):
    z = zscore(x) #np.abs()
    return z

def execute_statement(sql, parameters=None, json=False):
    if parameters is not None:
        if json:
            response = rds_client.execute_statement(
                secretArn=db_credentials_secret_store_arn,
                database=database_name,
                resourceArn=db_cluster_arn,
                sql=sql,
                parameters=parameters,
                formatRecordsAs='JSON'
            )
        else:
            response = rds_client.execute_statement(
                secretArn=db_credentials_secret_store_arn,
                database=database_name,
                resourceArn=db_cluster_arn,
                sql=sql,
                parameters=parameters
            )
    else:
        if json:
            response = rds_client.execute_statement(
                secretArn=db_credentials_secret_store_arn,
                database=database_name,
                resourceArn=db_cluster_arn,
                sql=sql,
                formatRecordsAs='JSON'
            )
        else:
            response = rds_client.execute_statement(
                secretArn=db_credentials_secret_store_arn,
                database=database_name,
                resourceArn=db_cluster_arn,
                sql=sql
            )
    return response
    
def dtype_mapping():
    return {'object' : 'TEXT',
        'int64' : 'INT',
        'float32' : 'FLOAT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'category' : 'TEXT',
        'timedelta[ns]' : 'TEXT'}
        
def gen_tbl_cols_sql(df, table):
    dmap = dtype_mapping()
    sql = "`dateUpdated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" #AUTO_INCREMENT 
    hdrs = df.dtypes.index
    hdrs_list = [(hdr, str(df[hdr].dtype)) for hdr in hdrs]
    for hl in hdrs_list:
        if hl[0] == 'profileId':
            sql += ", `{0}` VARCHAR(250) PRIMARY KEY".format(hl[0])
        else:
            sql += ", `{0}` {1}".format(hl[0], dmap[hl[1]])
    return "CREATE TABLE IF NOT EXISTS {0} ({1})".format(table, sql)
    
# Function to split the dataframe into chunks
def split_dataframe(df, chunk_size=100):
    num_chunks = ceil(len(df) / chunk_size)
    return (df[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks))

# Function to create SQL parameter sets for batch inserting or updating rows
def create_sql_parameter_sets(rows):
    sql_parameter_sets = []

    for _, row in rows.iterrows():
        row_params = [
            {'name': col.replace(' ', '_'), 'value': {'stringValue': str(row[col])}} for col in row.index
        ]
        sql_parameter_sets.append(row_params)

    return sql_parameter_sets
    
def read_partitioned_parquet_dataset(s3_path, partition=False):
    s3 = S3FileSystem()

    if partition != False:
        parquet_files = []
        # Iterate over all provided partitions (years)
        for year in partition:
            # List Parquet files in the directory for the specific year
            year_files = [f"s3://{file}" for file in s3.glob(f"{s3_path.rstrip('/')}/latterYear={year}/*.parquet")]
            parquet_files.extend(year_files)
        print("FILES LOCATED:", parquet_files)
    else:
        # List all Parquet files in the directory
        parquet_files = [f"s3://{file}" for file in s3.glob(f"{s3_path.rstrip('/')}/**/*.parquet")]
        print("FILES LOCATED (ALL):", parquet_files)

    dataset = pq.ParquetDataset(parquet_files, filesystem=s3)
    
    # Read the entire dataset into a single DataFrame
    combined_df = dataset.read_pandas().to_pandas()
    return combined_df
    
### CONFIG ###
# retrieve client id passed from STEP FUNCTION map as parameter
try:
    args = getResolvedOptions(sys.argv, ['client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'current_seasons'])
    client_id = args['client_id']
except:
    args = getResolvedOptions(sys.argv, ['default_client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'current_seasons'])
    client_id = args['default_client_id']
print(f"Client ID: {client_id}")
    
# retrieve reset indication (set manually in clients.csv)
# s3 = S3FileSystem()
clients = pd.read_csv('s3://traits-app/settings/clients.csv')
client_row = clients[clients['id'].astype('str') == str(client_id)]
reset_data = int(client_row['reset_data'].iloc[0])
leagues = ast.literal_eval(client_row['league_names'].iloc[0])
write_files = True

# define RDS access credentials
rds_client = boto3.client('rds-data')
database_name = 'traitsproddb'
db_cluster_arn = args['db_cluster_arn']
db_credentials_secret_store_arn = args['db_credentials_secret_store_arn']

# other args
current_seasons = ast.literal_eval(args['current_seasons']) # Note: not read from CSV, set at league level
write_files = True
s3_path = 's3://traits-app/silver-cleaned/3/parquet/' # league filters done at client per client level

print(f"Current seasons: {current_seasons} Leagues: {leagues}")

# Define subsets of demographics list for dtype enforcement

demographics_ls = ['profileId',
                    'playerId',
                    'competitionId',
                    'seasonId', #partition key
                   'aggregationPeriod',
                    'startYear',
                    'endYear',
                    'age',
                    'playerName',
                    'teamName',
                    'teamId',
                    'seasonName',
                    'competitionName',
                    'competitionShortName',
                    'fullName',
                    'nationality',
                    'birthDate',
                    'positionGroup',
                    'positionName',
                    'positionAbbreviation',
                    'teamSeason',
                    'playerTeamSeason',
                    'playerTeamSeasonCompetition',
                    'totalMinutesInSample',
                    'sampleSize',
                    'totalMinutesForSeason',
                    'appearancesForSeason']
                    
integer_columns = ['startYear',
'endYear',
'age',
'totalMinutesInSample',
'sampleSize',
'totalMinutesForSeason',
'appearancesForSeason']

string_columns = ['profileId',
'playerId',
'aggregationPeriod',
'competitionId',
'seasonId',
'playerName',
'teamName',
'teamId',
'seasonName',
'competitionName',
'competitionShortName',
'fullName',
'nationality',
'birthDate',
'positionGroup',
'positionName',
'positionAbbreviation',
'teamSeason',
'playerTeamSeason',
'playerTeamSeasonCompetition']

# define constants
base_constant = 2.5
apps_cutoff = 2
two_matches = ["AFLW", "Coates League Girls", "U18 Boys Championships", "U18 Girls Championships"]

# load in weights table
try:
    weights = pd.read_csv('s3://traits-app/settings/weights/{}.csv'.format(client_id)).fillna(0)
    #weights['statName'] = [s+' Inverted' if i == 1 else s for s,i in list(zip(weights['statName'],weights['inverse']))]
except FileNotFoundError:
    print("weights csv file not found")
except pd.errors.EmptyDataError:
    print("weights csv is empty")

# define stats
stats_ls = list(weights['statName'].unique())
stats_ls_inv = [s for s,i in list(zip(weights['statName'],weights['inverse'])) if i == 1] #[s for s in stats_ls if ' Inverted' in s]
print(stats_ls)
print(stats_ls_inv)

### LOAD AND PREPARE DATA ###

print("Updating ID: ", client_id)

# TODO: make robust to transition between years
print("RESET PARAMETER ==", reset_data)
if int(reset_data) == 0:
    data = read_partitioned_parquet_dataset(s3_path, partition=current_seasons)
    print('Updating current seasons: ({}): '.format(current_seasons), data.shape)
else:
    data = read_partitioned_parquet_dataset(s3_path)
    print('Resetting all seasons: ', data.shape)
    
print("COLUMNS ON READ:", data.columns.values)

# filter for leagues
data = data.loc[data["competitionName"].isin(leagues)]

print(f"Original shape {data.shape}")
    
# TODO: address hardcoded logic here
# duplicate columns to be inverted
data['Hit Out Sharked From'] = data['Hit Out Sharked'] # TODO: fix weights.csv input
data['Hit Out Sharked Inverted'] = data['Hit Out Sharked'] # TODO: fix weights.csv input
for inv_stat in stats_ls_inv:
    data[inv_stat] = data[inv_stat.replace(" Inverted","")]

# remove artifacts
data.columns = [c.replace(u'\xa0', u' ') for c in data.columns.values]

# remove double quotations if exist
for col in data.columns.values:
    if data[col].dtype == 'object':
        data[col] = data[col].astype('str')
        #data[col] = data[col].str.replace("''", "'")
        
# fill NAs
data = data.fillna(0)

# filter for games thresholds
enough_games = ((data["appearances"] > apps_cutoff) | 
                ((data["appearances"] > 1) & (data["competitionName"].isin(two_matches)))
               )
print(f'Removing {data.loc[~enough_games].shape[0]} players who have not played enough games')
data = data.loc[enough_games]
print("Eligible players per league")
print(data.groupby(["competitionName", "seasonName"]).nunique()["playerTeamSeason"])

missing_data = data[data["Disposal"] == 0]
games_played = Counter(data['appearances'])
print(f'Removing {missing_data.shape[0]} players who do not have enough disposals')
print(f'Of those, {games_played[1.0]} have 1 appearance, the remaining have more')
# data = data[data["Disposal"] != 0] #TODO: reintroduce remove zero disposal players

# # Keep the row for each player with more appearances in duplicate cases
# print("DATA COLS:", data.reset_index().columns.values)
# if "playerTeamSeason" not in data.reset_index().columns.values:
#     data = data.reset_index().rename(columns={"index":"playerTeamSeason"})
#     print(data[['playerTeamSeason']].head(5))
    
data = data.sort_values(by=['playerTeamSeason', 'appearances']).drop_duplicates(subset=['playerTeamSeason'], keep='last') #.reset_index()


print(f"Eligible shape {data.shape}")
print(f"Missing Position Count {data.loc[~data['posAbbr'].isin(weights['POS'].unique())].shape}")

# convert stats to z-scores per league-season
temp = data.copy()

# TEMP!!! Reassign wingers
temp['posAbbr'] = np.where(temp['detailedPosition'] == 'Wing', 'W', temp['posAbbr'])

### NEW LOGIC FOR APPENDING PLAYERS WITH NEW ASSIGNED POSITIONS AND CHANGING DEFAULT POSITIONS ###
# Load the csv containing the player position data
# load in positions amendments table
try:
    player_positions = pd.read_csv(f's3://traits-app/deployments/{client_id}/position_assignments.csv').astype('str')
    player_positions = player_positions.replace('nan', np.nan)
except FileNotFoundError:
    print("position_assignments.csv file not found")
except pd.errors.EmptyDataError:
    print("position_assignments.csv is empty")
    
# isolate rows with a change primary position
def find_subset(df1, df2):
    
    print(f"Duplicates in df1: {df1[df1['playerTeamSeason'].duplicated(keep=False)].shape}")
    print(f"Duplicates in df2: {df2[df2['playerTeamSeason'].duplicated(keep=False)].shape}")
    
    df1.drop_duplicates(subset=['playerTeamSeason'], keep = 'last', inplace=True)
    df2.drop_duplicates(subset=['playerTeamSeason'], keep = 'last', inplace=True)
    
    # Merge the two DataFrames on column A
    merged_df = df1.merge(df2[['playerTeamSeason', 'position1']], on='playerTeamSeason', how = 'inner')

    # Filter the merged DataFrame where column B differs
    subset_df = merged_df.loc[merged_df['posAbbr'] != merged_df['position1']]
    
    return subset_df
    
# define subframe of players to update
player_positions = player_positions.rename(columns={'Player':'playerTeamSeason'})
primary_position = player_positions[player_positions['position1'].notna()].sort_values(by='playerTeamSeason')

# find subset of all players which have a new primary position assigned
changed_position_data = find_subset(temp, primary_position) #.sort_values(by='playerName')

if (changed_position_data.shape[0] > 0):
    # map position changes
    print(f"Reassigning {changed_position_data.shape[0]} player positions.")
    
    mapping1 = dict(zip(primary_position['playerTeamSeason'], primary_position['position1']))
    changed_position_data['posAbbr'] = changed_position_data['playerTeamSeason'].map(mapping1)
    
    print("Updating main dataframe with new positions...")
    
    temp = pd.concat([temp, changed_position_data]).drop_duplicates(subset=['playerTeamSeason', 'competitionName'], keep='last')
    
    print(f"New shape: {temp.shape}")

# convert stats into z-scores by position, comp, season
stats_ls_zs = ['zs_'+stat for stat in stats_ls]
temp[stats_ls_zs] = temp.groupby(['posAbbr', 'competitionName', 'seasonName'])[stats_ls].transform(z_score)

# invert all of the stats that need inverting
for stat in stats_ls_inv:
     temp['zs_'+stat] = temp['zs_'+stat]*-1
        
print("Z-scores converted")

# fill NAs for z-scores, this accounts for Position-Comp-Season groups with no variance that have returned NAs
temp = temp.fillna(0)

### NEW POSITION ASSIGNMENT LOGIC ###
# Loads a csv containing specified positions for each player
# Creates a new row for each player with a changed primary, or additional second, or third
# Calculates the z-score for each player's second position based on the mean and standard deviation of that position

# Calculate mean and standard deviation for stats columns
mean_values = data.groupby(['posAbbr', 'competitionName', 'seasonName'])[stats_ls].mean()
std_values = data.groupby(['posAbbr', 'competitionName', 'seasonName'])[stats_ls].std()

# filter to changes only
second_position = player_positions[player_positions['position2'].notna()].sort_values(by='playerTeamSeason')
third_position = player_positions[player_positions['position3'].notna()].sort_values(by='playerTeamSeason')

# Create new rows for each player's changed or added positions
# Do this by grabbing those rows from the main dataframe and editing them to contain the alternate position
# TODO: assumes names unique within teams

mask2 = (
    (temp['playerTeamSeason'].isin(second_position['playerTeamSeason']))
)
mask3 = (
    (temp['playerTeamSeason'].isin(third_position['playerTeamSeason']))
)

second_position_data = temp[mask2].sort_values(by='playerName')
third_position_data = temp[mask3].sort_values(by='playerName')
    
# only apply if there are changes to be made
if (second_position_data.shape[0] > 0) or (third_position_data.shape[0] > 0):
    print(f"Appending {second_position_data.shape[0]} second player positions.")
    print(f"Appending {third_position_data.shape[0]} third player positions.")
    
    # Create a mapping of player id to new positions and then apply
    mapping2 = dict(zip(second_position['playerTeamSeason'], second_position['position2']))
    mapping3 = dict(zip(third_position['playerTeamSeason'], third_position['position3']))
    
    second_position_data['posAbbr'] = second_position_data['playerTeamSeason'].map(mapping2)
    third_position_data['posAbbr'] = third_position_data['playerTeamSeason'].map(mapping3)
    
    # Change the player ID to maintain uniqueness on it (except changed primary positions)
        # changed_position_data['playerTeamSeason'] = changed_position_data['playerTeamSeason'] + ' ' + changed_position_data['posAbbr']
    second_position_data['playerTeamSeason'] = second_position_data['playerTeamSeason'] + ' ' + second_position_data['posAbbr']
    third_position_data['playerTeamSeason'] = third_position_data['playerTeamSeason'] + ' ' + third_position_data['posAbbr']
    
    def add_player_duplicate(new_pos):
        # Calculate z-scores for the new instance
        position_index = (
            new_pos['posAbbr'].tolist()[0],
            new_pos['competitionName'].tolist()[0],
            new_pos['seasonName'].tolist()[0]
        )
        new_pos[stats_ls_zs] = (new_pos[stats_ls] - mean_values.loc[position_index]) / std_values.loc[position_index]
        return new_pos
            
    new_positions = []
    
    for _, row_df in second_position_data.groupby(second_position_data.index):
        # Access row data using row_df
        try:
            new_positions.append(add_player_duplicate(row_df))
        except KeyError:
            print(f"Index may not exist for {row_df}. Player position pair not appended.")
        
    for _, row_df in third_position_data.groupby(third_position_data.index):
        # Access row data using row_df
        try:
            new_positions.append(add_player_duplicate(row_df))
        except KeyError:
            print(f"Index may not exist for {row_df}. Player position pair not appended.")
        
    if len(new_positions) > 0:
        new_positions_df = pd.concat(new_positions, ignore_index=True)
        temp = pd.concat([temp, new_positions_df])
    
### END OF PLAYER POSITIONS LOGIC ###
    
print(f"Reweighting commenced on df shape {temp.shape}")
temp.to_csv('s3://traits-app/gold-transformed/{}/concat_output.csv'.format(client_id)) # write for debug

# drop non-standard positions
temp = temp.loc[temp['posAbbr'].isin(weights['POS'].unique())]

print(f"A.Aliir PAFC 2023 GD in playerTeamSeason values: {'A.Aliir PAFC 2023 GD' in temp['playerTeamSeason'].to_list()}")
print("SHAPE AFTER DROPPING NON-STANDARD POSITIONS", temp.shape)


# drop any players with nulls for any z-score values
temp = temp.fillna(0)
temp.replace([np.inf, -np.inf], np.nan, inplace=True)
temp.dropna(subset=stats_ls_zs, inplace=True)
print(f"A.Aliir PAFC 2023 GD in playerTeamSeason values: {'A.Aliir PAFC 2023 GD' in temp['playerTeamSeason'].to_list()}")

# generate metric scores from statistic z-scores and stat weights
weights2 = weights.copy()
weights2['statName'] = 'zs_'+weights2['statName']

player_stats = pd.DataFrame({})
for pos in weights['POS'].unique():
    print(pos)
    posgrp = weights2[weights2['POS'] == pos]
    metric_list = weights2['metricName'].unique()
    posgrp_data = temp.copy().loc[temp['posAbbr'] == pos]

    for metric in metric_list:
        metric_factors = posgrp[['statName', 'statWgt']].loc[posgrp['metricName']==metric].set_index(['statName'])
        stat_list = metric_factors.index.tolist()
        # display(metric_factors)
        posgrp_data[metric] = sum([posgrp_data[col] * metric_factors.at[col, 'statWgt'] for col in stat_list]) + base_constant

    player_stats = pd.concat([player_stats, posgrp_data])

temp = temp.merge(player_stats[['playerTeamSeason'] + metric_list.tolist()], on = 'playerTeamSeason', how = 'left')
print(temp.shape)
print(f"A.Aliir PAFC 2023 GD in playerTeamSeason values: {'A.Aliir PAFC 2023 GD' in temp['playerTeamSeason'].to_list()}")

# generate 'traits' scores by weighted sum of component metrics
trait_ratings = pd.DataFrame({})
for pos in weights['POS'].unique():
    print(pos)
    posgrp = weights[weights['POS']==pos]
    trait_list = weights['traitName'].unique()
    posgrp_data = temp.copy().loc[temp['posAbbr']==pos]

    for trait in trait_list:
        trait_factors = posgrp[['metricName', 'metricWgt']].loc[posgrp['traitName']==trait]\
        .drop_duplicates(subset=['metricName']).set_index(['metricName'])
        metric_list = trait_factors.index.tolist()
        # display(trait_factors)
        posgrp_data[trait] = sum([posgrp_data[col] * trait_factors.at[col, 'metricWgt'] for col in metric_list])

    trait_ratings = pd.concat([trait_ratings, posgrp_data])

temp = temp.merge(trait_ratings[['playerTeamSeason'] + trait_list.tolist()], on = 'playerTeamSeason', how = 'left')
print(f"A.Aliir PAFC 2023 GD in playerTeamSeason values: {'A.Aliir PAFC 2023 GD' in temp['playerTeamSeason'].to_list()}")
print(temp.shape)

# generate 'rating' score by weighted sum of component set(traits)
final_ratings = pd.DataFrame({})
for pos in weights['POS'].unique():
    print(pos)
    posgrp = weights[weights['POS']==pos]
    posgrp_data = temp.copy().loc[temp['posAbbr']==pos]

    rating_factors = posgrp[['traitName', 'traitWgt']].drop_duplicates(subset=['traitName']).set_index(['traitName'])
    trait_list = rating_factors.index.tolist()
    # display(rating_factors)
    posgrp_data['Rating'] = sum([posgrp_data[col] * rating_factors.at[col, 'traitWgt'] for col in trait_list])

    final_ratings = pd.concat([final_ratings, posgrp_data])
        
final = temp.merge(final_ratings[['playerTeamSeason', 'Rating']], on = 'playerTeamSeason', how = 'left')
print(f"A.Aliir PAFC 2023 GD in playerTeamSeason values: {'A.Aliir PAFC 2023 GD' in temp['playerTeamSeason'].to_list()}")

# TODO: add code to check distribution of calculated values
# summary_stats = final[['Rating'] + trait_list + metric_list].describe()
# for col in summary_stats:
#     if summary_stats[col]['std'] == 0:
#         raise ValueError(f'ERROR: {col} has zero variance')
#     if (summary_stats[col]['min'] < -1.5):
#         print(f'WARNING: {col} has a value more than 4 standard deviations less than the mean')
#     if (summary_stats[col]['max'] > 6.5):
#         print(f'WARNING: {col} has a value more than 4 standard deviations greater than the mean')

### TEMP RENAMES FOR v2 ### 
# This is a light temporary adapter to force compatibility between the v1 pipeline and the v2 schema 
# It will be deprecated once the entire v2 pipeline is in place
final['playerTeamSeasonCompetition'] = final['playerTeamSeason']+final['competitionName']
final['profileId'] = final['playerTeamSeasonCompetition'] + '-' + final['posAbbr']
final['aggregationPeriod'] = 'season'
final['startYear'] = final['seasonName'].apply(lambda x: ''.join(re.findall(r'\d', x)))
final['endYear'] = final['startYear'].copy()
final['totalMinutesForSeason'] = 0
final['appearancesForSeason'] = final['appearances'].copy()
final['positionAbbreviation'] = final['detailedPosition']
final['fullName'] = final['playerName'].copy()
final['competitionShortName'] = final['competitionName'].copy()
final['minutes'] = 0
final['competitionId'] = final['competitionName']
final['teamId'] = final['teamName']
final['seasonId'] = final['seasonName']
final['playerId'] = final['playerName'] + ' ' + ['DOB']
final = final.rename(columns={'posAbbr':'positionGroup',
    'detailedPosition':'positionName',
    'appearances':'sampleSize',
    'minutes':'totalMinutesInSample',
    'DOB':'birthDate'})

### WRITE OUTPUT ###
# Clean data types and send to output
raw_stat_list = weights['statName'].unique().tolist()
zs_stat_list = weights2['statName'].unique().tolist()
metric_list = weights['metricName'].unique().tolist()
trait_list = weights['traitName'].unique().tolist()
float_ls = ['Rating'] + trait_list + metric_list + raw_stat_list + zs_stat_list

# check floats
for col in float_ls:
    final[col] = round(final[col].astype('float'),2)
    assert np.issubdtype(final[col].dtype, np.floating), f'{col} column should be a float'
# check strings
for col in string_columns:
    final[col] = final[col].astype(object)
    assert final[col].dtype == 'object', f'{col} column should be a string'
# check ints
for col in integer_columns:
    try:
        final[col] = final[col].astype('int64')
        assert final[col].dtype == 'int64', f'{col} column should be an integer.'
    except:
        print(f"Col {col} could not be converted to an integer without NAs")
        final[col] = pd.to_numeric(final[col],errors='coerce',downcast='integer')

final = final[string_columns + integer_columns + float_ls].sort_values(by='playerTeamSeason')
# we can't have NAs in there
final = final.dropna(subset=float_ls)
      
# remove % signs
# final.columns = [c.replace("%","Pct") for c in final.columns.values]

# write output
if write_files:
    final.to_csv('s3://traits-app/gold-transformed/{}/output.csv'.format(client_id))
    print("CSV and parquet files saved with shape: ", temp.shape)

# write to Aurora RDS
print("Commencing write to RDS...")

# drop extra columns if exist
try:
    final = final.drop(columns=[c for c in final.columns.values if "Unnamed" in c])
except:
    pass
        
print("Executing SCHEMA statements...")

# check if schema exists

alter_statements = [
        f"CREATE SCHEMA IF NOT EXISTS id_{client_id}",
        gen_tbl_cols_sql(final, f'id_{client_id}.Output'),
        f"ALTER TABLE id_{client_id}.Output CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        f"""ALTER TABLE id_{client_id}.Output
        ADD COLUMN fullNameNormalised TEXT  GENERATED ALWAYS AS (REPLACE(REPLACE(fullName, 'ø', 'o'), 'Ø', 'O')) STORED,
        ADD COLUMN playerNameNormalised TEXT  GENERATED ALWAYS AS (REPLACE(REPLACE(playerName, 'ø', 'o'), 'Ø', 'O')) STORED,
        ADD FULLTEXT KEY playerFullTextIndex (fullNameNormalised, playerNameNormalised, teamName, competitionName, positionName, seasonName),
        ADD KEY idx_season_competition (seasonName(25), competitionName(255)),
        ADD KEY idx_player_team (playerId(255), teamName(255));""",
        f"""CREATE FULLTEXT INDEX IF NOT EXISTS playerUniversalFullTextIndex ON id_{client_id}.Output(fullNameNormalised, playerNameNormalised, teamName, competitionName);"""
        ]
        
i = 0
for attempt in range(5):
    try:
        response = execute_statement('SHOW DATABASES LIKE "id_{}"'.format(client_id))
        #print(response)
        if len(response['records']) >= 1:
            print("Database at id_{} exists.".format(client_id))
            mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
            print("Existing shape: ", execute_statement("SELECT COUNT(`playerTeamSeason`) FROM id_{}.Output".format(client_id))['records'][0][0].values())
            mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
            mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
            i+=1
        else:
            print("Database at id_{} does not exist. Initializing table.".format(client_id))
            for statement in alter_statements:
                execute_statement(statement)
            print("Database id_{} created.".format(client_id))
            mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
            mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
            mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
    except:
        time.sleep(1)
        print("Retrying RDS connection.")
    if i == 1:
        break
    else:
        print("RDS connection could not be initiated.")

# batch upload frame to RDS
# Load your dataframe
write_df = final.drop_duplicates(subset=['profileId'])

# ensure identical schema and same order !IMPORTANT
write_columns = list(write_df.columns)
write_dtypes = write_df.dtypes.to_dict()

# if write_columns != mysql_columns:
#     print("TABLE: ", write_columns)
#     print("DATABASE:", mysql_columns)
#     raise ValueError("Columns in DataFrame do not match columns in MySQL table")
# else:
#     print("...COLUMN NAMES VALID")

# check dtypes are valid (but not necessarily equal due to server side optimisation)
# TODO: fix dtypes check
dtype_map = dtype_mapping()
print(dtype_map)
for i, (col, d) in enumerate(write_dtypes.items()):
    print(d, mysql_dtypes[i])
    try:
        if dtype_map[str(d)] != mysql_dtypes[i]:
            raise ValueError(f"Data type of column '{d}' in DataFrame does not match data type in MySQL table")
    except:
        print(f"error reading dtype {d} for col {write_columns[i]}")
print("...DTYPES CHECKED")

# Define the unique id column
table_name = "id_{}.Output".format(client_id)
database = 'traitsproddb'
unique_id_col = 'profileId'

# Create ON DUPLICATE KEY UPDATE string
update_str = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in write_df.columns if col != unique_id_col])
update_str += ', `dateUpdated` = CURRENT_TIMESTAMP'
vals = [f':{col.replace(" ", "_")}'for col in write_df.columns]

# Exclude dateUpdated from the columns list
columns = ', '.join([f"`{col}`" for col in write_df.columns])

# SQL statement
sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(vals)}) ON DUPLICATE KEY UPDATE {update_str}"

start = time.time()

r=0

# Iterate through the dataframe chunks
for i, chunk in enumerate(split_dataframe(write_df)):
    sql_parameter_sets = create_sql_parameter_sets(chunk)
    try:
        response = rds_client.batch_execute_statement(
            resourceArn=db_cluster_arn,
            secretArn=db_credentials_secret_store_arn,
            database=database_name,
            sql=sql,
            parameterSets=sql_parameter_sets
        )
        r += len(response["updateResults"])
        # if i % 5 == 0:
        #     print(sql)
        #     print(sql_parameter_sets)
        #     print(chunk.head())
        print(f'Number of records updated: {len(response["updateResults"])}')
        print("RUNNING:", r)
        print("Time to insert dataframe: ", (time.time() - start))
    except Exception as e:
        print("Chunk failed. Check logs.")
        print("Error:", e)
        chunk.to_csv(f's3://traits-app/silver-cleaned/3/csv/failed_chunk_{r}.csv')