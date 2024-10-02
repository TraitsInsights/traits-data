### Libraries and config
import ast
import re
import sys
import json
import boto3
import pandas as pd
import pyspark.sql.functions as F
from datetime import datetime
from time import time
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import explode, concat, col, lit, udf, expr, array_max, collect_list, sum as _sum, year, to_date, first, struct, input_file_name, regexp_extract, when, concat, countDistinct, rank
from pyspark.sql.types import IntegerType, StringType, ArrayType, StructType
from datetime import datetime, timedelta
from s3fs import S3FileSystem
import warnings

### Initialise clients
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3 = S3FileSystem()

### Variables ###

# Retrieve arguments passed from STEP FUNCTION map as parameter
# NB: Wyscout Data Warehouse is centralised, so client_id can be used as a proxy for production / other replication
try:
    args = getResolvedOptions(sys.argv, ['client_id', 'seasons_to_update'])
    client_id = args['client_id']
except:
    args = getResolvedOptions(sys.argv, ['default_client_id', 'seasons_to_update'])
    client_id = args['default_client_id']
    
# Convert to strings
seasons_to_update = [str(s) for s in ast.literal_eval(args['seasons_to_update'])]
    
# Threshold for valid games contributing to a profile sample
playtime_threshold = 33

# Columns to pass to output in addition to features
meta_cols = ['profileId',
'playerId',
'teamId',
'competitionId',
'seasonId',
'seasonPartition',
'startYear',
'endYear',
'age',
'playerName',
'teamName',
'seasonName',
'seasonDisplayName',
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
]

integer_columns = ['startYear',
'endYear',
'age',
'totalMinutesInSample',
'sampleSize',
'totalMinutesForSeason',
'appearancesForSeason']

string_columns = ['profileId',
'playerId',
'teamId',
'competitionId',
'seasonId',
'seasonPartition',
'playerName',
'teamName',
'seasonName',
'seasonDisplayName',
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
'aggregationPeriod']

### Helper functions ###

def flatten_struct(schema, prefix=None):
    """Generate a list of column expressions that flatten a DataFrame schema with prefixed nested column names."""
    field_exprs = []
    for field in schema.fields:
        name = field.name
        dtype = field.dataType
        current_col = f"`{name}`" if prefix is None else f"`{prefix}`.`{name}`"
        current_prefix = name if prefix is None else f"{prefix}_{name}"
        
        if isinstance(dtype, StructType):
            # If the field is a StructType, recurse to flatten it
            nested_exprs = flatten_struct(dtype, prefix=current_prefix)
            field_exprs.extend(nested_exprs)
        else:
            # For non-struct fields, alias the column with its prefixed name
            field_exprs.append(col(current_col).alias(current_prefix))
    return field_exprs

def calculate_age(birthDate, year):
    today = datetime.today()
    try:
        birthDate_obj = datetime.strptime(birthDate, '%Y-%m-%d')
        if today.year == year:
            age = today.year - birthDate_obj.year
            if today.month < birthDate_obj.month or (today.month == birthDate_obj.month and today.day < birthDate_obj.day):
                age -= 1
        else:
            age = int(year) - birthDate_obj.year
        return age
    except:
        return None
        
def get_national_team(team_id, name):
    return national_teams.get(str(team_id), name)

### Pre-load merge tables [retrieved information] ###

# Position mapping dictionary
try:
    with s3.open(f's3://traits-app/settings/positions_map.json', 'r', encoding='utf-8') as f:
        pos_key = json.load(f)['WYSCOUT']
    pos_key = {k: oldk for oldk, oldv in pos_key.items() for k in oldv}
except FileNotFoundError:
    print("positions_map.json file not found")
except ValueError:
    print("positions_map.json file is not in the expected format")
    
# Competition grouping dictionary
# e.g. college conferences to be grouped by division
try:
    with s3.open(f's3://traits-app/settings/competition_groups.json', 'r', encoding='utf-8') as f:
        competition_groups = json.load(f)
except FileNotFoundError:
    print("competition_groups.json file not found")
except ValueError:
    print("competition_groups.json file is not in the expected format")
    
# Competitions [id, competition name]
comps_df = pd.read_csv('s3://traits-app/settings/wyscout_competitions.csv', encoding='utf-8-sig')
comps_df['displayName'] = comps_df['displayName'].astype('str')
comps_df['wyId'] = comps_df['wyId'].astype('int')

# National teams lookups
with s3.open('s3://traits-app/bronze-raw/wyscout/national_teams.json', 'r', encoding='utf-8') as file:
    national_teams = json.load(file)

# Teams [id, team name]
teams_data = []

for seasonId in seasons_to_update:
    
    try:
        teams_path = f's3://traits-app/bronze-raw/wyscout/seasons/{seasonId}/teams.json'

        with s3.open(teams_path, 'r') as file:
            team_json = json.load(file)

        teams_data.extend([team_json])
        
    except Exception as e:
        print(f"Teams json file could not be found for season: {seasonId}: {e}")
    
teams_df = pd.json_normalize(teams_data, record_path='teams',
    meta=[['season', 'wyId'],
    ['season', 'name'],
    ['season', 'startDate'],
    ['season', 'endDate'],
    ['season', 'active'],
    ['season', 'competitionId']
    ]).rename(columns={'wyId':'teamId','season.wyId':'seasonId'}) #ERROR: used to add .drop_duplicates(subset='teamId') meaning only current seasons kept
    
### TEMP: solution only valid if we are taking the team + conference name approach

# Rename displayName to Division / Competition Group name

conference_names = comps_df.set_index('wyId')['displayName'].to_dict()

teams_df['officialName'] = teams_df.apply(
    lambda row: f"{row['name']} ({conference_names[int(row['season.competitionId'])]})" 
                if competition_groups.get(str(row['season.competitionId']),False) != False  
                else row['officialName'], 
    axis=1
)

# Rename displayName to Division / Competition Group name
comps_df['displayName'] = comps_df.apply(
    lambda row: competition_groups[str(row['wyId'])] if competition_groups.get(str(row['wyId']),False) != False else row['displayName'], 
    axis=1
)

### ETL PROCESSING ###    

print(f"Parsing seasons: {seasons_to_update}")

start = time()

# Read player statistics from json
player_s3_path = "s3://traits-app/bronze-raw/wyscout/seasons/{" + ",".join(seasons_to_update) + "}/player-stats/*.json"
df_matches = spark.read.json(player_s3_path)
df_matches = df_matches.select(explode(col("players")).alias("player")).select("player.*")

# Specify meta columns to include
include_cols = ['seasonId', 'competitionId', 'roundId', 'matchId', 'playerId', 'positions',
                'player.birthDate', 'player.currentNationalTeamId', 'player.currentTeamId', 'player.shortName',
                'player.firstName', 'player.middleName', 'player.lastName', 'player.foot',
                'player.height', 'player.weight', 'player.gender',
                'player.imageDataURL', 'player.birthArea.name', 'player.role', 'player.status']

# Generate the column expressions for the DataFrame
flatten_exprs = [col(name) for name in include_cols] +\
                flatten_struct(df_matches.schema["total"].dataType, prefix="total") +\
                flatten_struct(df_matches.schema["average"].dataType, prefix="average") +\
                flatten_struct(df_matches.schema["percent"].dataType, prefix="percent")

# Apply the expressions to select and alias the columns
df_matches = df_matches.select(*flatten_exprs)

# Drop NA playerIds
df_matches = df_matches.where(df_matches.playerId.isNotNull())

# List columns in the DataFrame
columns = df_matches.columns

# TODO: refactor pipeline to avoid renames and add display name col instead
# Iterate over each column and rename if necessary
for col_name in columns:
    # Check if the column name starts with 'total'
    if col_name.startswith('total'):
        # Rename column by removing the 'total' prefix
        new_col_name = col_name[6:]
        df_matches = df_matches.withColumnRenamed(col_name, new_col_name)

# Map the positionGroup to the Wyscout abbreviation the position map
broadcast_pos_map = spark.sparkContext.broadcast(pos_key)

def map_position_to_abbr(position_name):
    return broadcast_pos_map.value.get(position_name, None)

map_position_to_abbr_udf = udf(map_position_to_abbr, StringType())

# Extract the *first listed positionName and positionCode* from Wyscout's positions dictionary
df_matches = df_matches.withColumn("primary_position", col("positions")[0])
df_matches = df_matches.withColumn("positionName", col("primary_position.position.name"))
df_matches = df_matches.withColumn("positionAbbreviation", col("primary_position.position.code"))
df_matches = df_matches.withColumn("positionGroup", map_position_to_abbr_udf(col("positionAbbreviation")))

# ### OPTIONAL check for unique player per match rows

# # Check to ensure there is only one row per player per match
# check_df = df_matches.groupBy("playerId", "matchId").count()

# # # Filter to find invalid rows
# invalid_rows_df = check_df.filter(check_df['count'] > 1)

# # # Show the shape of the invalid filtered rows and a sample of those rows
# if invalid_rows_df.count() > 0:
#     print(f"Number of total rows: {df_matches.count()}")
#     print(f"Number of invalid rows: {invalid_rows_df.count()}")
#     # invalid_sample = invalid_rows_df.orderBy(F.col("playerId")).select("playerId", "matchId").show(5, truncate=False)
#     raise ValueError("There are multiple rows for some players in some matches")

# ### 

# Create rows that will not apply gametime thresholds, marked as "ANY" in positionGroup, positionName, positionCode
duplicated_df = df_matches.withColumn("positionGroup", F.lit("ANY")).withColumn("positionName", F.lit("ANY")).withColumn("positionAbbreviation", F.lit("ANY"))

# Append to original positions frame
print(f"Shape before merging ANY frame: {df_matches.count()}")
df_matches = df_matches.unionByName(duplicated_df)
print(f"Shape after merging ANY frame: {df_matches.count()}")

# Drop rows where the total matchtime was under the threshold, but keep all "ANY" rows
# NB: this IGNORES that occasionally total game minutes will be over the threshold, but time spent in primary position will not be
df_split = df_matches.filter(
    (col('minutesOnField') > playtime_threshold) | (col('positionGroup') == "ANY")
)
print(f"Shape after dropping ineligible matches for all players frame: {df_split.count()}")

# ### OPTIONAL check for unique player per match rows

# # Check to ensure there is only two rows per player per match
# check_df = df_split.groupBy("playerId", "matchId").count()

# # # Filter to find invalid rows
# invalid_rows_df = check_df.filter(check_df['count'] > 2)

# # # Show the shape of the invalid filtered rows and a sample of those rows
# if invalid_rows_df.count() > 0:
#     print(f"Number of total rows (after union): {df_matches.count()}")
#     print(f"Number of invalid rows (after union): {invalid_rows_df.count()}")
#     # invalid_sample = invalid_rows_df.orderBy(F.col("playerId")).select("playerId", "matchId").show(5, truncate=False)
#     raise ValueError("There are more than two rows for some players in some matches (after union)")

# ###

### Retrieve all fixtures with lineups for season
# TODO: fixtures and lineups are held in memory - this approach creates a chokepoint

lineups = []
fixture_errors = []
total_fixtures = 0
    
# TODO: bulk read in Spark
for seasonId in seasons_to_update:
    s3_path = f's3://traits-app/bronze-raw/wyscout/seasons/{seasonId}/fixtures.json'
    try:
        with s3.open(s3_path, 'r', encoding='utf8') as file:
            fixtures_json = json.load(file)
    except FileNotFoundError as e:
        print(f"Season match file was not found at {e}")
        continue
    total_fixtures += len([f for f in fixtures_json['matches'] if f['match']['hasDataAvailable']==True])

    for match in fixtures_json['matches']:
        try:
            for team, info in match['match']['teamsData'].items():
                for player in info['formation']['lineup']:
                    lineups.append({
                                    'matchId':match['matchId'],
                                    'playerId':player['playerId'],
                                    'teamId':team
                                   })
                for player in info['formation']['bench']:
                    lineups.append({
                                    'matchId':match['matchId'],
                                    'playerId':player['playerId'],
                                    'teamId':team
                                   })
        except:
            fixture_errors.append(match['matchId'])

lineups_pdf = pd.DataFrame(lineups)
lineups_pdf['teamId'] = lineups_pdf['teamId'].astype('int')
df_lineups = spark.createDataFrame(lineups_pdf.drop_duplicates(subset=['matchId','playerId']))

# Join the lineups with main dataframe to add teamId for each player
df_split = df_split.join(df_lineups, on=["matchId", "playerId"], how="inner")

# Join competition name information to the main df
df_split = df_split.join(spark.createDataFrame(comps_df[['wyId', 'name', 'displayName']].rename(columns={'wyId':'competitionId',
                                                                                                         'name':'competitionShortName'}
                                                    ).drop_duplicates()), on="competitionId", how="left")

# Join team name information to the main df
df_split = df_split.join(spark.createDataFrame(teams_df[['teamId', 'seasonId', 'officialName', 'season.name', 'season.startDate', 'season.endDate']]),
                                on=['teamId', 'seasonId'], how="left")
                                
### Rename columns
df_split = df_split.withColumnRenamed("displayName", "competitionName")\
                         .withColumnRenamed("officialName", "teamName")\
.withColumnRenamed("season.startDate", "seasonStart")\
.withColumnRenamed("season.endDate", "seasonEnd")\
.withColumnRenamed("season.name", "seasonDisplayName")\
                         .withColumnRenamed("shortName", "playerName")\
                         .withColumnRenamed("birthDate", "birthDate")\
                         .withColumnRenamed("weight", "playerWeight")\
                         .withColumnRenamed("height", "playerHeight")\
                         .withColumnRenamed("name", "birthArea")

### Create additional columns
df_split = df_split.withColumn("startYear", year(to_date(col("seasonStart"), "yyyy-MM-dd")))
df_split = df_split.withColumn("endYear", year(to_date(col("seasonEnd"), "yyyy-MM-dd")))
df_split = df_split.withColumn("seasonName", col("endYear")) # standardise season names to their end calendar year
df_split = df_split.withColumn("seasonPartition", col("seasonId"))
df_split = df_split.withColumn("fullName", F.concat_ws(" ", df_split["firstName"],  df_split["lastName"]))
df_split = df_split.withColumn("teamSeason", F.concat(df_split["teamName"], F.lit(" "), df_split["seasonName"]))
df_split = df_split.withColumn("playerTeamSeason", F.concat(df_split["playerName"], F.lit(" "), df_split["teamSeason"]))
df_split = df_split.withColumn("playerTeamSeasonCompetition", F.concat(df_split["playerTeamSeason"], F.lit(" "), df_split["competitionShortName"]))
df_split = df_split.withColumn("profileId", F.concat(df_split["playerId"].cast("string"), 
                                             df_split["teamId"].cast("string"),
                                             df_split["seasonId"].cast("string"), 
                                             df_split["competitionId"].cast("string"),
                                             df_split["positionGroup"]))
                                             
# Apply nationality UDF
get_national_team_udf = udf(get_national_team, StringType())
df_split = df_split.withColumn("nationality", get_national_team_udf(col("currentNationalTeamId"), col("birthArea")))

# Apply age UDF
calculate_age_udf = udf(calculate_age, IntegerType())
df_split = df_split.withColumn("age", calculate_age_udf("birthDate", "endYear"))

### Create features using feature store aggregation recipes

# Read in feature formula map
retrieved_features = pd.read_csv(f's3://traits-app/settings/feature_store_wyscout.csv') # NOTE: creates all features (not a subset)

# Initialise SQL query
sql_query_features = "SELECT m.profileId, COUNT(m.profileId) AS sampleSize, SUM(m.minutesTagged) AS totalMinutesInSample"
                        
# Iterate through the DataFrame to populate the dictionary
for _, row in retrieved_features.iterrows():
    feature_name = row['feature_name']
    if pd.notna(row['base_sql']):  # Ensure there is a base SQL to work with
        sql_query_features += f", {row['base_sql']} AS {feature_name}"
    else:
        print(ValueError(f"Valid base_sql does not exist for feature '{feature_name}'"))
            
# Finish the SQL query by specifying the source and grouping condition
sql_query_features += " FROM {table_name} m GROUP BY m.profileId"

# Apply the SQL statement
df_split.createOrReplaceTempView("matches_season")
df_features = spark.sql(sql_query_features.format(table_name='matches_season'))

# Identify aggregationPeriod
df_features = df_features.withColumn("aggregationPeriod", lit("season"))

### Repeat for last four and eight games
# TODO: wrap into function which accepts last_x parameter
# Filter for top 4 and top 8 matches (for each profile, not by gameweek)
windowSpec = Window.partitionBy("profileId").orderBy(col("matchId").desc()) # Assumes match ids are ascending
df_ranked = df_split.withColumn("rank", rank().over(windowSpec))
df_last4 = df_ranked.filter(col("rank") <= 4)
df_last8 = df_ranked.filter(col("rank") <= 8)

df_last4.createOrReplaceTempView("matches_4")
df_last8.createOrReplaceTempView("matches_8")

df_features_4 = spark.sql(sql_query_features.format(table_name="matches_4"))
df_features_8 = spark.sql(sql_query_features.format(table_name="matches_8"))

df_features_4 = df_features_4.withColumn("aggregationPeriod", lit("last_four"))
df_features_8 = df_features_8.withColumn("aggregationPeriod", lit("last_eight"))

# NB: pay attention to order of operations here
# Concatenate all the DataFrames
df_features = df_features.unionByName(df_features_4).unionByName(df_features_8)

### Remerge meta info to aggregated DataFrame
# NB: there may be duplicates on position_name here, hence information loss
# TODO: make positionName assignment non-random
df_features = df_features.join(df_split.select(meta_cols).dropDuplicates(['profileId']), on="profileId", how="left")

# NOTE: there will be duplicate profileIds at this point
# Update profileId (represents profile participation and aggregation)
df_features = df_features.withColumn("profileId", concat(df_features["profileId"], lit("-"), df_features["aggregationPeriod"]))

### OPTIONAL: merge total player-season minutes and appearance counts to profiles
# NB: this does not apply any thresholding on contributing matches (see upstream)
any_df = df_features.filter((df_features.positionGroup == 'ANY') & (df_features.aggregationPeriod == 'season')) 
any_df = any_df.withColumnRenamed("totalMinutesInSample", "totalMinutesForSeason")\
                            .withColumnRenamed("sampleSize", "appearancesForSeason")

df_features = df_features.join(any_df.select("playerId", "teamId", "seasonId", "competitionId", "totalMinutesForSeason", "appearancesForSeason"),
                               on=["playerId", "teamId", "seasonId", "competitionId"], how="left")

### Enforce output table data types
# NB: all unspecified columns will be floats for simplicity

for column_name in df_features.columns:
    if column_name in integer_columns:
        df_features = df_features.withColumn(column_name, col(column_name).cast('integer'))
    elif column_name in string_columns:
        df_features = df_features.withColumn(column_name, col(column_name).cast('string'))
    else:
        df_features = df_features.withColumn(column_name, col(column_name).cast('double'))
        
# ### Delete existing partition data
# def delete_partition_subdirectory(bucket, prefix, partition_key, partition_value):
#     s3 = boto3.resource('s3')
#     bucket = s3.Bucket(bucket)
#     partition_path = f"{prefix}{partition_key}={partition_value}/"
    
#     # Delete the entire partition directory
#     bucket.objects.filter(Prefix=partition_path).delete()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Basic write DataFrame to S3 with overwrite mode
df_features.write \
    .mode("overwrite") \
    .partitionBy("seasonPartition") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(f"s3://traits-app-wyscout/silver-cleaned/{client_id}")
    
# ### UNCOMMENT FOR DEBUG RUNS IF NEEDED
# df_features.coalesce(1) \
#     .write \
#     .mode("overwrite") \
#     .format("csv") \
#     .option("header", "true") \
#     .save("s3://traits-app-wyscout/silver-cleaned/debug_output")

# # Write to Glue Catalog (updating the catalog information)

# for season_id in seasons_to_update:
#     delete_partition_subdirectory("traits-app-wyscout", f"silver-cleaned/{client_id}", 'seasonId', season_id)

# dynamic_frame = DynamicFrame.fromDF(df_features, glueContext, "dynamic_frame")
    
# # print final schema
# print("Final schema:")
# dynamic_frame.printSchema()

# glueContext.write_dynamic_frame.from_catalog(
#     frame=dynamic_frame,
#     database="wyscout",
#     table_name="wyscout-player-data",
#     transformation_ctx="s3output"
# )

# s3output = glueContext.getSink(
#   path=f"s3://traits-app-wyscout/silver-cleaned/{client_id}",
#   connection_type="s3",
#   updateBehavior="UPDATE_IN_DATABASE",
#   partitionKeys=['seasonPartition'],
#   compression="snappy",
#   enableUpdateCatalog=True,
#   transformation_ctx="s3output",
# )
# s3output.setCatalogInfo(
#   catalogDatabase="wyscout", catalogTableName="wyscout-player-data"
# )
# s3output.setFormat("glueparquet")
# s3output.writeFrame(dynamic_frame)

# TODO: replace with structured logging and run outputs throughout
print(f"Writes for {seasons_to_update} successful. Elapsed time: {time() - start} seconds.")
print(f"Estimated {total_fixtures} matches parsed.")
print(f"{len(fixture_errors)} fixture errors.")

job.commit()