"""
AWS Glue ETL Script for Upserting Data from a structured data warehouse to MySQL Aurora DB

Known as "Gold" level transformations in the bronze-silver-gold appproach.

This script reads partitioned Parquet data from an S3 bucket, performs checks, applies tiered weighting as per a client's Traits framework, and upserts the data into a MySQL Aurora database.

The script uses Python Shell for data processing and RDS Data API for database connectivity.

Prerequisites:
1. AWS Glue job with appropriate IAM roles and permissions.
2. S3 bucket containing partitioned Parquet files.
3. All parameters specified and passed to this Glue job's environment.
3. [OPTIONAL, ELSE INITIATED] MySQL Aurora database associated with the CLIENT_ID parameter with table(s) to perform upserts.
4. JDBC driver for MySQL uploaded to an S3 bucket.

Steps:
1. [IF USING SPARK] Initialize AWS Glue context and Spark session.
2. Load parameters
3. Read and transform data from S3.
3. Perform upsert operations into MySQL Aurora DB using `INSERT ... ON DUPLICATE KEY UPDATE`.

Parameters:
- CLIENT_ID: Id of the customer associated with this job.
- seasons_to_process: a List of unique seasons (seasons within a competition) to parse
- db_cluster_arn: ARN of the MySQL Aurora database.
- db_credentials_secret_store_arn: Secret for the MySQL Aurora database.
- database_name: Target database in the MySQL Aurora database.
- base_constant: The constant added to the standard normal distribution to form the mean of the final calculated scores.
- min_apps: The minimum appearances to be considered for season level statistics.
- min_mins: The minimum appearances to be considered for season level statistics. Both apps and mins thresholds must be met.
- demographics: a List containing field names that MUST be present.

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

### CONFIG ###
# Retrieve client id passed from STEP FUNCTION map as parameter
try:
    args = getResolvedOptions(sys.argv, ['client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'seasons_to_process', 'data_provider'])
    client_id = args['client_id']
    data_provider = args['data_provider']
except:
    args = getResolvedOptions(sys.argv, ['default_client_id', 'db_cluster_arn', 'db_credentials_secret_store_arn', 'seasons_to_process', 'default_data_provider'])
    client_id = args['default_client_id']
    data_provider = args['default_data_provider']

# Define RDS access credentials
rds_client = boto3.client('rds-data')
database_name = 'traitsproddb'
db_cluster_arn = args['db_cluster_arn']
db_credentials_secret_store_arn = args['db_credentials_secret_store_arn']
seasons_to_process = ast.literal_eval(args['seasons_to_process'])
s3 = S3FileSystem()

### PARAMETERS
min_apps = 3.5
min_mins = 300
base_constant = 2.5
profiles_threshold = 0
# TODO: review requirements for v2 app and refactor writer accordingly
demographics_ls = ['profileId',
                    'playerId',
                    'competitionId',
                    'seasonId', #partition key
                    'teamId',
                   'aggregationPeriod',
                    'startYear',
                    'endYear',
                    'age',
                    'playerName',
                    'teamName',
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
                    
# retrieve any competition restrictions
# s3 = S3FileSystem()
clients = pd.read_csv('s3://traits-app/settings/clients.csv')
clients.index = clients['id'].astype('int')
try:
    reset_data = clients.at[int(client_id), 'reset_data']
except KeyError:
    reset_data = 1
except Exception as e:
    raise e
try:
    use_ids = 1
    leagues = ast.literal_eval(clients.at[int(client_id), 'league_ids'])
except:
    try:
        use_ids = 0
        leagues = ast.literal_eval(clients.at[int(client_id), 'league_names'])
    except:
        use_ids = 0 
        leagues = False
                    
### HELPER FUNCTIONS

def flatten_list(input_list):
    ### Flattens a nested list into a single list of strings if necessary.
    if any(isinstance(i, list) for i in input_list):
        flattened_list: List[int] = [item for sublist in input_list for item in sublist]
        return flattened_list
    else:
        return input_list

def z_score(x) -> np.ndarray: # DEPRECATED. See apply_z_score.
    ### Calculates the z-scores of the input data array or list.
    z: np.ndarray = zscore(x)
    return z

def dtype_mapping():
    ### Maps pandas dataframe data types to MySQL data types
    return {'object' : 'TEXT',
        'int64' : 'INT',
        'float32' : 'FLOAT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'category' : 'TEXT',
        'timedelta[ns]' : 'TEXT'}

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

def split_dataframe(df, chunk_size=100):
    ### Splits a Pandas dataframe into chunks. Returns a iterator of dataframe chunks.
    num_chunks = ceil(len(df) / chunk_size)
    return (df[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks))


def create_sql_parameter_sets(rows):
    """
    Creates SQL parameter sets for a batch of rows from a Pandas dataframe,
    correctly handling null values and data types.
    """
    sql_parameter_sets = []

    for _, row in rows.iterrows():
        row_params = []
        for col in row.index:
            param_name = col.replace(' ', '_')
            if pd.isnull(row[col]):
                row_params.append({'name': param_name, 'value': {'isNull': True}})
            else:
                value = row[col]
                if isinstance(value, int):
                    row_params.append({'name': param_name, 'value': {'longValue': value}})
                elif isinstance(value, float):
                    row_params.append({'name': param_name, 'value': {'doubleValue': value}})
                else:
                    row_params.append({'name': param_name, 'value': {'stringValue': str(value)}})
        sql_parameter_sets.append(row_params)

    return sql_parameter_sets

# TODO: vectorize and refactor
def apply_z_score(row, stat):
    # Calculate z-score for a given raw statistic based of the corresponding season-level mean and std.
    group = (row['positionGroup'], row['competitionName'], row['seasonName'])
    if (group, stat) in distribution_dict:
        mean, std = distribution_dict[(group, stat)]
        if pd.isnull(row[stat]):
            print(f"{group} for stat: {stat} is null.")
            return 0 # return the mean for both ALL nulls
        elif std == 0:
            print(f"{group} {stat} has zero variance for the season.")
            return np.nan
        else:
            return (row[stat] - mean) / std
    else:
        print(f"{group} not found in Z-score distributions")
        return np.nan  # Return NaN if no distribution info is available

def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    ### Prepares string for SQL compatibility by removing double quotes
    
    for col in df.columns.values:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace("''", "'")
            
    return df

def convert_types(df: pd.DataFrame, float_ls: list, demographics_ls: list, integer_ls: list) -> pd.DataFrame:
    """
    Validates and converts columns in the DataFrame to the correct data types. Raises ValueError if conversion fails due to unexpected inputs.
    - Columns in float_ls should be converted to float32 (allow nulls unless column name starts with 'zs_').
    - Columns in demographics_ls should be converted to string, except those in integer_ls which should be converted to int.
    """
    # Convert float32 columns
    for col in float_ls:
        try:
            if col.startswith('zs_'):
                df[col] = df[col].astype('float32')
                assert not df[col].isnull().any(), f"Column '{col}' contains NaNs but is a z-score field."
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
        except Exception as e:
            raise ValueError(f"Column '{col}' could not be converted to float32: {e}")

    # Convert demographics columns
    for col in demographics_ls:
        if col in integer_ls:
            try:
                df[col] = pd.to_numeric(df[col], errors='raise').astype('int')
            except Exception as e:
                raise ValueError(f"Column '{col}' could not be converted to int: {e}")
        else:
            try:
                df[col] = df[col].astype('str')
            except Exception as e:
                raise ValueError(f"Column '{col}' could not be converted to string: {e}")

    return df

def generate_updater_sql(write_df: pd.DataFrame, unique_id_col: str, table_name: str) -> str:
    """
    Creates an SQL statement for inserting rows, including an ON DUPLICATE KEY UPDATE clause.

    Args:
        write_df (pd.DataFrame): The DataFrame containing the data to write.
        unique_id_col (str): The name of the unique ID column.
        table_name (str): The name of the target table.

    Returns:
        str: The SQL statement for insertion.
    """
    # Create ON DUPLICATE KEY UPDATE string
    update_str = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in write_df.columns if col != unique_id_col])
    update_str += ', `dateUpdated` = CURRENT_TIMESTAMP'
    
    # Create values placeholders
    vals = [f':{col.replace(" ", "_")}' for col in write_df.columns]
    
    # Exclude dateUpdated from the columns list
    columns = ', '.join([f"`{col}`" for col in write_df.columns])
    
    # SQL statement
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(vals)}) ON DUPLICATE KEY UPDATE {update_str}"
    
    return sql
    
### Prepare parameters

# Flatten seasons to process in this batch
seasons_to_process = [str(s) for s in flatten_list(seasons_to_process)]

# Load in weights table, specific to client_id
try:
    weights = pd.read_csv('s3://traits-app/settings/weights/{0}.csv'.format(client_id))
except FileNotFoundError:
    print("Weights csv file not found")
except pd.errors.EmptyDataError:
    print("Weights csv is empty")

# Retrieve features required to be processed
stats = weights['statName'].unique().tolist()

# Retrieve position groups to be processed
positions = weights['POS'].unique().tolist()

# Define features to be inversed
inverse = list(weights['statName'].loc[weights['inverse']==1])
print("INVERSE STATS:", inverse)

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
'teamId',
'playerName',
'teamName',
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

### TEMP COMP ID REMAPPINGS
# Remap to single competition id post-benchmarking
group_remaps = {
    "NCAA D1":12,
    "NCAA D2":13,
    "NCAA D3":14,
    "NCAA D1 W":15,
    "NCAA D2 W":16,
    "NCAA D3 W":17
    } # TODO: Brazil, Serie D etc.
        
### MAIN LOOP over seasons

print(f"Filtering for leagues: {leagues}")

for season in seasons_to_process:
    
    if data_provider == "wyscout":
        try:
            data = pd.read_parquet(f's3://traits-app-wyscout/silver-cleaned/5/seasonPartition={season}/')
        except:
            print(f"Season {season} does not exist in data warehouse.")
            continue
    elif data_provider == "statsbomb":
        try:
            data = pd.read_parquet(f's3://traits-app/silver-cleaned/8/seasonPartition={season}/')
            print(data.shape)
        except:
            print(f"Season {season} does not exist in data warehouse.")
            continue
        
    # filter for leagues
    # TODO: avoid loading data before valid leagues check by integrating in step function
    if leagues:
        if use_ids==1:
            data = data[data["competitionId"].astype('int').isin(leagues)]
        else:
            data = data[data["competitionName"].isin(leagues)]
            
    if data.shape[0] == 0:
        print(f"No data found for season {season}... Skipping ...")
        continue
    
    # Check demographic columns present
    data = data[stats + demographics_ls]
    
    # Filter for selected positions
    data = data.loc[data['positionGroup'].isin(positions)]
    
    # TODO: enforce no nulls on critical columns
    data = data.dropna(subset=['playerName', 'teamName', 'seasonName', 'competitionName'])
    
    # define competition name for season for competition id remapping after benchmarking
    competition_name = list(data['competitionName'].unique())[0]
    assert len(data['competitionName'].unique()) == 1
    
    # TODO: enforce no duplicates at silver level
    data = data.drop_duplicates(subset=['profileId'],keep='first')
    
    # TEMP: until we implement round ranges
    data = data.loc[data['aggregationPeriod']=='season']
    
    # Enforce demographics and features dtypes

    # Check strictly no duplicates
    duplicate_count = data.duplicated(subset=['profileId']).sum()
    assert duplicate_count == 0 #, print(data[['playerName', 'profileId']].loc[data.duplicated()].sample(5))

    
    # Pipe for minor transformations
    # TODO: check this func
    # Remove double quotations in string fields
    # data = clean_strings(data)
    
    # Drop ineligible players as per low sample thresholds
    # WARNING: may be compatibility issues if aggPeriod < min_apps
    data = data.loc[(data['sampleSize'] >= min_apps) & (data['totalMinutesInSample'] >= min_mins)]
    print('Size of frame after minimum thresholds applied:', data.shape)
    
    # Handle leagues with too few games (no eligible profiles)
    if data.shape[0] == 0:
        print("Zero eligible entries, skipping season.")
        continue
    if data.shape[0] <= profiles_threshold:
        print("Eligible entries are under the profiles_threshold, skipping season.")
        continue
    
    # Calculate mean and standard deviation for the 'season' aggregation level
    season_stats = data.loc[(data['aggregationPeriod'] == 'season') & (data['positionGroup'] != 'ANY')]\
        .groupby(['positionGroup', 'competitionName', 'seasonName'])[stats]\
        .agg(['mean', 'std'], skipna=True).reset_index()

    # Enforce demographics and features dtypes
    
    # Check strictly no duplicates
    duplicate_count = data.duplicated(subset=['profileId']).sum()
    assert duplicate_count == 0
    
    # Pipe for minor transformations
        
    # Create a dictionary to hold mean and std values for each feature and group
    distribution_dict = {}
    for _, row in season_stats.iterrows():
        group = (row['positionGroup'].values[0], row['competitionName'].values[0], row['seasonName'].values[0])
        for stat in stats:
            distribution_dict[(group, stat)] = (row[(stat, 'mean')], row[(stat, 'std')])
    
    # Apply Z-score transformation
    for stat in stats:
        z_stat_name = 'zs_' + stat
        data[z_stat_name] = data.apply(apply_z_score, axis=1, args=(stat,))
        data[z_stat_name] = data[z_stat_name].fillna(0) # retrospectively give nulls the average value
        if stat in inverse:
            data[z_stat_name] = data[z_stat_name]*-1

    # WRITE CSV
    # data.to_csv(f's3://traits-app-wyscout/silver-cleaned/{season}.csv/')
    
    # # Check no columns are completely nulls
    # completely_null_columns = [col for col in data.columns if data[col].isnull().all()]
    # assert not completely_null_columns, f"DataFrame contains completely null columns: {completely_null_columns}"
    
    # # Check no NAs in z-scores
    # zs_columns_with_nulls = [col for col in data.columns if col.startswith('zs_') and data[col].isnull().any()]
    # assert not zs_columns_with_nulls, f"DataFrame contains NaN values in z-score columns: {zs_columns_with_nulls}"   

    ### Apply dot multiplications based on default weights
    
    # Generate 'metric' scores from z-scores and corresponding weights
    zweights = weights.copy()
    zweights['statName'] = 'zs_'+zweights['statName']
    
    player_stats = pd.DataFrame({})
    for pos in weights['POS'].unique():
        posgrp = zweights[zweights['POS'] == pos]
        metric_list = zweights['metricName'].unique()
        posgrp_data = data.copy().loc[data['positionGroup'] == pos]
    
        for metric in metric_list:
            metric_factors = posgrp[['statName', 'statWgt']].loc[posgrp['metricName']==metric].set_index(['statName'])
            stat_list = metric_factors.index.tolist()
            posgrp_data[metric] = sum([posgrp_data[col] * metric_factors.at[col, 'statWgt'] for col in stat_list]) + base_constant
    
        player_stats = pd.concat([player_stats, posgrp_data])
    
    data = data.merge(player_stats[['profileId'] + metric_list.tolist()], on = 'profileId', how = 'left', validate='one_to_one') # merges must be 1:1 on primary key
    
    # Generate 'traits' scores by weighted sum of component metrics
    trait_ratings = pd.DataFrame({})
    for pos in weights['POS'].unique():
        posgrp = weights[weights['POS']==pos]
        trait_list = weights['traitName'].unique()
        posgrp_data = data.copy().loc[data['positionGroup']==pos]
    
        for trait in trait_list:
            trait_factors = posgrp[['metricName', 'metricWgt']].loc[posgrp['traitName']==trait]\
                .drop_duplicates(subset=['metricName']).set_index(['metricName']) # WARNING: metric names must be unique
            metric_list = trait_factors.index.tolist()
            posgrp_data[trait] = sum([posgrp_data[col] * trait_factors.at[col, 'metricWgt'] for col in metric_list])
    
        trait_ratings = pd.concat([trait_ratings, posgrp_data])
    
    data = data.merge(trait_ratings[['profileId'] + trait_list.tolist()], on = 'profileId', how = 'left', validate = 'one_to_one')
    
    # Generate 'rating' score by weighted sum of component set(traits)
    final_ratings = pd.DataFrame({})
    for pos in weights['POS'].unique():
        posgrp = weights[weights['POS']==pos]
        posgrp_data = data.copy().loc[data['positionGroup']==pos]
    
        rating_factors = posgrp[['traitName', 'traitWgt']].drop_duplicates(subset=['traitName']).set_index(['traitName'])
        trait_list = rating_factors.index.tolist()
        posgrp_data['Rating'] = sum([posgrp_data[col] * rating_factors.at[col, 'traitWgt'] for col in trait_list])
    
        final_ratings = pd.concat([final_ratings, posgrp_data])
    
    final = data.merge(final_ratings[['profileId', 'Rating']], on = 'profileId', how = 'left', validate='one_to_one')
    
    ### Data validation for final dataframe to write

    # Clean data types and send to output
    raw_stat_list = weights['statName'].unique().tolist()
    zs_stat_list = zweights['statName'].unique().tolist()
    metric_list = weights['metricName'].unique().tolist()
    trait_list = weights['traitName'].unique().tolist()
    float_ls = ['Rating'] + trait_list + metric_list + raw_stat_list + zs_stat_list
    
    # Enforce standard dtypes
    # final = convert_types(final, float_ls, string_columns, integer_columns) # TODO: duplicated with below loop
    
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
    
    # Cut unnecessary columns
    final = final[string_columns + integer_columns + float_ls]
    
    ### WRITE OUTPUT
    # TODO: align with v2 requirements and schema
    
    ### Write to RDS
    
    print("Executing CREATE SCHEMA statements...")
    
    # Batch upload frame to RDS
    # Ensure identical schema and same order
    write_df = final.copy()
    write_columns = list(write_df.columns)
    write_dtypes = write_df.dtypes.to_dict()
    
    print(write_dtypes)

    ### TEMP: reassign competition id for 'grouped' competitions e.g. college
    if group_remaps.get(competition_name, False) != False:
        write_df['competitionId'] = group_remaps[competition_name]
        
    # Create table if not existing
    # TODO: retries?
    ### Creates a table if not exists, including index columns
    print(f"Writing to table associated with client id: {client_id}")
    table_name = f"id_{client_id}.Output"
    
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
        f"CREATE FULLTEXT INDEX playerUniversalFullTextIndex ON id_{client_id}.Output(fullNameNormalised, playerNameNormalised, teamName, competitionName);"
        ]
        
    # Check if the database exists
    response = execute_statement('SHOW DATABASES LIKE "id_{}"'.format(client_id))
    if len(response['records']) >= 1:
        print("Database at id_{} exists.".format(client_id))
        response = execute_statement(f'SHOW TABLES IN id_{client_id} LIKE "Output"')
        if len(response['records']) >= 1:
            print("Table id_{}.Output exists.".format(client_id))
            mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
            mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
            mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
        else:
            for statement in alter_statements:
                execute_statement(statement)
            print("Table id_{}.Output created.".format(client_id))
            mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
            mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
            mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
    else:
        for statement in alter_statements:
            execute_statement(statement)
        print("Database id_{} created.".format(client_id))
        mysql_schema = execute_statement("DESCRIBE id_{}.Output".format(client_id))['records']
        mysql_columns = [row[0]['stringValue'] for row in mysql_schema][1:]
        mysql_dtypes = [row[1]['stringValue'] for row in mysql_schema][1:]
    
        print(f"Database {client_id} created.")
        print(mysql_dtypes)

    # TODO: wrap dtype checks into a function
    # WARNING: schema evolution may result in redundant columns persisting, and outdating of historical data
    # Check if all write columns are encapsulated in RDS columns
    if not all(col in mysql_columns for col in write_columns):
        print("TABLE: ", write_columns)
        print("DATABASE:", mysql_columns)
        raise ValueError("Columns in DataFrame do not match columns in MySQL table")
    else:
        print("...COLUMN NAMES VALID")
    
    # TODO: reinstate
    # # Check dtype alignment
    # write_dtypes = {col: dtype_mapping()[str(dtype)] for col, dtype in write_df.dtypes.items()}
    # mysql_column_types = dict(zip(mysql_columns, mysql_dtypes))
    # for col, dtype in write_dtypes.items():
    #     if col in mysql_column_types and dtype != mysql_column_types[col]:
    #         raise ValueError(f"Data type of column '{col}' in DataFrame does not match data type in MySQL table (DataFrame: {dtype}, MySQL: {mysql_column_types[col]})")
    # print("DTYPES... CHECKED")
    
    # Define the primary key
    unique_id_col = 'profileId'
    
    # Generate sql
    sql = generate_updater_sql(write_df, unique_id_col, table_name)
    
    # Initialise counters
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
        print("Records:", r)
        print("Time to insert dataframe: ", (time.time() - start))
    
    print(f'Season {season} updated.')
    # TODO: log season stats
    
# print("Creating fact tables (if not exists)...")
# # TODO: will not currently add new seasons or competitions as they are added
# table_statements = [
#     f"""CREATE TABLE IF NOT EXISTS id_{client_id}.competition
#     AS SELECT DISTINCT competitionName AS name FROM id_{client_id}.Output;""",
#     f"""CREATE TABLE IF NOT EXISTS id_{client_id}.season
#     AS SELECT DISTINCT seasonName AS name FROM id_{client_id}.Output;""",
#     ]
# for sql in table_statements:
#     execute_statement(sql)