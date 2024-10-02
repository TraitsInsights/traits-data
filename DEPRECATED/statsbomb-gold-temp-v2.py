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
import datetime as dt

from s3fs import S3FileSystem
from scipy.stats import zscore
from math import ceil
from awsglue.utils import getResolvedOptions

### CONFIG ###
# retrieve client id passed from STEP FUNCTION map as parameter
try:
    args = getResolvedOptions(sys.argv, ['client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'current_seasons']) #, 'current_year'
    client_id = args['client_id']
except:
    args = getResolvedOptions(sys.argv, ['default_client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'current_seasons']) # , 'current_year'
    client_id = args['default_client_id']
print(f"Client ID: {client_id}")

# retrieve reset indication (set manually in clients.csv)
# s3 = S3FileSystem()
clients = pd.read_csv('s3://traits-app/settings/clients.csv')
clients.index = clients['id'].astype('int')
reset_data = clients.at[int(client_id), 'reset_data']
try:
    use_ids = 1
    leagues = ast.literal_eval(clients.at[int(client_id), 'league_ids'])
except:
    use_ids = 0
    leagues = ast.literal_eval(clients.at[int(client_id), 'league_names'])

# define RDS access credentials
rds_client = boto3.client('rds-data')
database_name = 'traitsproddb'
db_cluster_arn = args['db_cluster_arn']
db_credentials_secret_store_arn = args['db_credentials_secret_store_arn']

# other args
# current_year = args['current_year']
current_seasons = [str(s) for s in ast.literal_eval(args['current_seasons'])]
write_files = True
s3_path = 's3://traits-app/silver-cleaned/{0}/parquet/'.format(client_id)

### PARAMETERS
min_apps = 3.5
min_mins = 300
base_constant = 2.5
# TODO: review requirements for v2 app and refactor writer accordingly
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

# load in weights table
try:
    weights = pd.read_csv('s3://traits-app/settings/weights/{}.csv'.format(client_id))
except FileNotFoundError:
    print("weights csv file not found")
except pd.errors.EmptyDataError:
    print("weights csv is empty")

stats = weights['statName'].unique().tolist()
print('unique metrics in weights file:', weights['metricName'].unique())

# define inverse stats
# inverse = weights['statName'].loc[weights['inverse']==1]
inverse = ['turnovers_90',
'dispossessions_90',
'failed_dribbles_90',
'sideways_pass_proportion',
'backward_pass_proportion',
'op_f3_sideways_pass_proportion',
'op_f3_backward_pass_proportion',
'errors_90',
'fouls_90',
'yellow_cards_90',
'red_cards_90']

# Define subsets of demographics list for dtype enforcement
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
    ### Creates a table replicating the local dataframe specifying profileId as the primary key.
    ### WARNING: ensure dataframe dtypes are enforced prior to function call
    dmap = dtype_mapping()
    sql = "`dateUpdated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP" # auto-generate timestamp
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

### LOAD AND PREPARE DATA ###

print("Updating ID: ", client_id)

# TODO: make robust to transition between years
print("RESET PARAMETER ==", reset_data)
if int(reset_data) == 1:
    data = read_partitioned_parquet_dataset(s3_path)
    print('Resetting all seasons: ', data.shape)
else:
    data = read_partitioned_parquet_dataset(s3_path, partition=current_seasons)
    print('Updating current seasons: ({}): '.format(current_seasons), data.shape)
    
# ensure primary key column playerTeamSeasonCompetition
if 'playerTeamSeasonCompetition' not in data.columns.values:
    print("MISSING PRIMARY KEY. COLUMNS:", data.columns.values)
    
print(data.columns.values)
   
 # filter for leagues
if leagues: # TODO: will default to all leagues if none specified
    print(f"Unique leagues: {len(list(data['competition_id'].unique()))}")
    print(data['competitionName'].unique())
    print(f"Filtering for leagues: {leagues}")
    if use_ids==1:
        data = data[data["competition_id"].astype('int').isin(leagues)]
    else:
        data = data[data["competitionName"].isin(leagues)]
        
print(f"Shape of loaded data. {data.shape}")

# fill NAs
# NOTE: handled correctly in v2
temp = data.copy()
temp = temp.fillna(0)

# remove duplicates on PTSC
# NOTE: handled correctly to allow for multiple positions in v2
data = data.drop_duplicates(subset=['playerTeamSeasonCompetition'], keep = 'last')
print(f"Shape of loaded data after duplicates dropped and league filters applied: {data.shape}")

# remove double quotations if exist
for col in data.columns.values:
    if data[col].dtype == 'object':
        data[col] = data[col].astype('str')
        data[col] = data[col].str.replace("''", "'")
    
# drop ineligible players as per low sample thresholds
temp = temp.loc[(temp['appearances'] >= min_apps) & (temp['minutes'] >= min_mins)]
print('Size of frame after min thresholds applied:', temp.shape)

# # rename players with multiple competitions in the same season
s = temp.groupby(['playerTeamSeason']).cumcount()
print('Players with multiple entries in the same season for the same team:', np.count_nonzero(s))
# rename multiple-team players
temp['playerTeamSeason'] = (temp['playerTeamSeason'] + s[s>0].astype(str)).fillna(temp['playerTeamSeason'])

# convert stats into z-scores by position, comp, season
stats_ls_zs = ['zs_'+stat for stat in stats]
temp[stats_ls_zs] = temp.groupby(['posAbbr', 'competitionName', 'seasonName'])[stats].transform(z_score)
# invert statistics
inverse_zs = ['zs_'+stat for stat in inverse]
temp[inverse_zs] = temp[inverse_zs]*-1
        
# fill NAs for z-scores, this accounts for Position-Comp-Season groups with no variance that have returned NAs
temp = temp.fillna(0)

### CARRY OUT DOT MULTIPLICATIONS ###

# generate 'metric' scores from statistic z-scores and stat weights
weights2 = weights.copy()
weights2['statName'] = 'zs_'+weights2['statName']

player_stats = pd.DataFrame({})
for pos in weights['POS'].unique():
    posgrp = weights2[weights2['POS'] == pos]
    metric_list = weights2['metricName'].unique()
    posgrp_data = temp.copy().loc[temp['posAbbr'] == pos]

    for metric in metric_list:
        metric_factors = posgrp[['statName', 'statWgt']].loc[posgrp['metricName']==metric].set_index(['statName'])
        stat_list = metric_factors.index.tolist()
        posgrp_data[metric] = sum([posgrp_data[col] * metric_factors.at[col, 'statWgt'] for col in stat_list]) + base_constant
        
    player_stats = pd.concat([player_stats, posgrp_data])

temp = temp.merge(player_stats[['playerTeamSeason'] + metric_list.tolist()], on = 'playerTeamSeason', how = 'left')

# generate 'traits' scores by weighted sum of component metrics
trait_ratings = pd.DataFrame({})
for pos in weights['POS'].unique():
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

# generate 'rating' score by weighted sum of component set(traits)
final_ratings = pd.DataFrame({})
for pos in weights['POS'].unique():
    posgrp = weights[weights['POS']==pos]
    posgrp_data = temp.copy().loc[temp['posAbbr']==pos]

    rating_factors = posgrp[['traitName', 'traitWgt']].drop_duplicates(subset=['traitName']).set_index(['traitName'])
    trait_list = rating_factors.index.tolist()
    # display(rating_factors)
    posgrp_data['Rating'] = sum([posgrp_data[col] * rating_factors.at[col, 'traitWgt'] for col in trait_list])

    final_ratings = pd.concat([final_ratings, posgrp_data])
        
final = temp.merge(final_ratings[['playerTeamSeason', 'Rating']], on = 'playerTeamSeason', how = 'left')

### DATA VALIDATION ###

### TEMP RENAMES FOR v2 ### 

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
        
# This is a light temporary adapter to force compatibility between the v1 pipeline and the v2 schema 
# It will be deprecated once the entire v2 pipeline is in place
print(final[['playerName', 'seasonName', 'teamName', 'competitionName', 'posAbbr', 'playerTeamSeasonCompetition']].dtypes)
print(final['posAbbr'].unique())
final['profileId'] = final['playerTeamSeasonCompetition'].astype('str') + '-' + final['posAbbr'].astype('str')
final['teamId'] = final['teamName'].astype('str') + '-' + final['competitionName'].astype('str')
final['aggregationPeriod'] = 'season'
final['startYear'] = final['seasonName'].map(lambda x: str(x).split("/")[0])
final['endYear'] = final['seasonName'].map(lambda x: str(x).split("/")[-1])
final['seasonName'] = final['endYear'].copy()
final['totalMinutesForSeason'] = final['minutes'].copy()
final['appearancesForSeason'] = final['appearances'].copy()
final['positionAbbreviation'] = final['detailedPosition']
final['fullName'] = final['playerName'].copy()
final['competitionShortName'] = final['competitionName'].copy()
final['age'] = final.apply(lambda row: age(row['DOB'], row['endYear']), axis=1)

final = final.rename(columns={'posAbbr':'positionGroup',
    'detailedPosition':'positionName',
    'appearances':'sampleSize',
    'minutes':'totalMinutesInSample',
    'player_id': 'playerId',
    'competition_id': 'competitionId',
    'season_id': 'seasonId',
    'DOB':'birthDate'
})

print(final[['playerName', 'seasonName', 'teamName', 'competitionName']].sample(5))

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

final = final[string_columns + integer_columns + float_ls].sort_values(by='playerTeamSeasonCompetition')
# we can't have NAs in there
final = final.dropna(subset=float_ls)

# TODO: add code to check distribution of calculated values
summary_stats = final[['Rating'] + trait_list + metric_list].describe()
for col in summary_stats:
    if summary_stats[col]['std'] == 0:
        raise ValueError(f'ERROR: {col} has zero variance')
    if (summary_stats[col]['min'] < -1.5):
        print(f'WARNING: {col} has a value more than 4 standard deviations less than the mean')
    if (summary_stats[col]['max'] > 6.5):
        print(f'WARNING: {col} has a value more than 4 standard deviations greater than the mean')

### WRITE OUTPUT ###
        
# write output for debugging
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
        
print(f"Leagues updating: {sorted(leagues)}")

print("Executing SCHEMA statements...")

# check if schema exists
i = 0

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
        
for attempt in range(5):
    try:
        response = execute_statement('SHOW DATABASES LIKE "id_{}"'.format(client_id))
        if len(response['records']) >= 1:
            print("Database at id_{} exists.".format(client_id))
            mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
            print("Existing shape: ", execute_statement("SELECT COUNT(`playerTeamSeason`) FROM id_{}.Output".format(client_id))['records'][0][0].values())
            mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
            mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
            i+=1
        else:
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
for chunk in split_dataframe(write_df):
    sql_parameter_sets = create_sql_parameter_sets(chunk)
    response = rds_client.batch_execute_statement(
        resourceArn=db_cluster_arn,
        secretArn=db_credentials_secret_store_arn,
        database=database_name,
        sql=sql,
        parameterSets=sql_parameter_sets
    )
    r += len(response["updateResults"])
    print(f'Number of records updated: {len(response["updateResults"])}')
    print("RUNNING:", r)
    print("Time to insert dataframe: ", (time.time() - start))

print("Creating fact tables...")
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

print("Updating fact tables with database id {}.".format(db_id))
for sql in gen_table_statements(db_id=client_id):
    execute_statement(sql)
    
print('Client ID: ', client_id, ' updated.')
# print("Updated shape: ", execute_statement("SELECT COUNT(`playerTeamSeason`) FROM id_{}.Output".format(client_id)))