"""
Created on Mon Apr 18 19:06:52 2022

@author: ascherta

This file does the ETL to create a fact table in the NBA Scoring star Schema.
THe resulting file is loaded using VS SSIS package.

##PEP8 80 Character Limit -----------------------------------------------------
"""

import os
import sqlalchemy as sal
from sqlalchemy import create_engine
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


Server = 'LAPTOP-KJGENM5N'
Database = 'NBADW'
Driver = 'ODBC Driver 17 for SQL Server'
db_conn = f'mssql://{Server}/{Database}?driver={Driver}'

engine = sal.create_engine(db_conn)
conn = engine.connect()


'''cwd = os.getcwd()
os.chdir(cwd + '\\SSISFiles')
cwd = os.getcwd()
source_dir = cwd + '\\DataFiles\\Source DataFiles\\'
data_dir = cwd + '\\DataFiles\\'''


# Load the GameDim info from the GameDim in the data warehouse.
gamedf = pd.read_sql_query("SELECT * FROM GameDim", conn)


sql_teamdf = 'SELECT team_dim_syn_key, team_name, season_start_year ' \
             'FROM TeamDim'


teamdf = pd.read_sql_query(sql_teamdf, conn)


sql_coach = 'SELECT head_coach_dim_syn_key' \
            ', coach_name' \
            ', coach_last_name' \
            ', team_initials' \
            ', team' \
            ', season_start_year' \
            ', coach_id_not_pk' \
            ' FROM HeadCoachDim'



coach_df = pd.read_sql_query(sql_coach, conn)

sql_player = 'SELECT player_name' \
             ', player_dim_syn_key' \
             ', season_start_year - 1 AS "career_start_year"' \
             ', season_end_year - 1 AS "career_end_year"' \
             ' FROM PlayerDim'

player_df = pd.read_sql_query(sql_player, conn)

#conn.close()
#print('Connection closed.')


df = pd.read_csv(source_dir + 'boxscore.csv')

# ---------------------------------------------------------------------------
# ---------------------- GAME ID ADD ----------------------------------------
# Add year from GameDim


merged_df = pd.merge(df, gamedf, how='left', on='game_id')
df_size = {'Initial Merge': len(merged_df)}

column_list = ['game_id', 'teamName', 'playerName', 'FG', 'FGA', '3P', '3PA',
               'MP', 'PTS', 'FT', 'FTA', 'winning_team', 'game_dim_syn_key',
               'game_date']

merged_df = merged_df[column_list]

# Add a column showing if the player/team represented by the record won
# the game. First get a bool value by comparing the winner to the team name
# then replace that with 0 or 1.
merged_df['win'] = merged_df['teamName'] == merged_df['winning_team']
merged_df['win'].replace({False: 0, True: 1}, inplace=True)

# ---------------------------------------------------------------------------
# ---------------------- Season ID ADD ----------------------------------------
# Use the date to lookup the calendar id and add it to merged_df
# Limit the number of records being pulled by only pulling dates on or later
# than the earliest game in gamedf

earliest = merged_df['game_date'].min()


sql_seasondf = 'SELECT season_date_dim_syn_key, full_date, ' \
               '[year] AS year_from_dateDim FROM SeasonDateDim ' \
               'WHERE full_date >= ' +"'"+ str(earliest)+"'"

season_df = pd.read_sql_query(sql_seasondf, conn)

# Merge the date synthetic key into the df.
merged_df = pd.merge(merged_df, season_df, how='left', left_on='game_date', \
                     right_on='full_date')



# Remove the season_df
# del season_df


# Create list of columns to keep and a new order.
column_list = ['playerName', 'teamName',  'FG', 'FGA', '3P', '3PA',
               'MP', 'PTS', 'FT', 'FTA', 'game_date', 'win',
               'game_dim_syn_key', 'game_id', 'season_date_dim_syn_key',
               'year_from_dateDim']

# Remove unneeded columns and re-arrange.
merged_df = merged_df[column_list]
df_size['After Season Date Add'] = len(merged_df)


# ---------------------------------------------------------------------------
# ---------------------- TEAM ID ADD ----------------------------------------
# Lookup team ID based on teamName and Year



# Do an inner merge to get rid of records outside of the date range.
merged_df = pd.merge(merged_df, teamdf, how='left',
                     left_on=['teamName', 'year_from_dateDim'],
                     right_on=['team_name', 'season_start_year'])

column_list = ['playerName', 'teamName',  'FG', 'FGA', '3P', '3PA',
               'MP', 'PTS', 'FT', 'FTA', 'game_date', 'win',
               'game_dim_syn_key', 'game_id', 'season_date_dim_syn_key',
               'year_from_dateDim', 'team_dim_syn_key']

merged_df = merged_df[column_list]

# Change the year column back to int from float.
#merged_df['team_dim_syn_key'] = merged_df['team_dim_syn_key'].astype('int')
df_size['After Team Add'] = len(merged_df)

# ---------------------------------------------------------------------------
# ---------------------- COACH ID ADD ----------------------------------------

# Pull data from HeadCoachDim

################################################################################
# Pull the '_game_lookup.csv'
################################################################################
coach_lookup_df = pd.read_csv(data_dir + '_game_lookup.csv')

################################################################################
################################################################################

merged_df = pd.merge(merged_df, coach_lookup_df, how='left',
                     left_on=['game_id', 'teamName'],
                     right_on=['game_id', 'Team'])

# Add the coach synthetic key from the DW dimension
#   Start by making coach_df smaller so the merge isn't as intensive

# Columns to drop.
coach_col = ['coach_name',
             'team_initials']

# Drop the columns
coach_df.drop(columns=coach_col, inplace=True)

# Merge coach synthetic key into merged_df on the coach name, team, and year

###############################################################################
#                               Merge
###############################################################################

merged_df = pd.merge(merged_df, coach_df, how='left',
                            left_on=['coach_last_name',
                                     'seasonStartYear'],
                            right_on=['coach_last_name',
                                      'season_start_year'],
                            indicator='coach_merge')
###############################################################################
#                               Problem Statement
###############################################################################
df_size['After Coach Add'] = len(merged_df)
# ---------------------------------------------------------------------------
# ---------------------- PLAYER ID ADD ----------------------------------------



# Can reduce the number of duplicates by looking at the year of the game
# Games outside of a players active year won't count
#'season_start_year' from merged_df
#

merged_df = pd.merge(merged_df, player_df, how='left', left_on='playerName',
                     right_on='player_name')
df_size['After Player Add'] = len(merged_df)


################################################NOT WORKING#############
# Find duplicate player DataFiles
dups = merged_df.duplicated(subset=['player_name',
                                    'game_dim_syn_key',
                                    'teamName'], keep=False)

# Add columns indicating if player/game/team combo is a duplicate to assist
# With finding common names like "Mike Smith" etc.
merged_df['duplicate_players'] = dups

duplicate_players = merged_df[merged_df['duplicate_players']==True]

# Add a column comparing 'seasonStartYear' with the range of
# career_start_year: career_end_year
merged_df = merged_df.assign(active_in_season=merged_df['seasonStartYear'].\
                             between(merged_df['career_start_year'], \
                                     merged_df['career_end_year'],\
                                     inclusive='both'))

# Remove players with 'True' flagged in duplicate and 'False' flagged in
# 'Active
merged_df = merged_df.drop(merged_df.index\
                               [(merged_df['duplicate_players'] == True) & \
                                (merged_df['active_in_season'] == False)])

# For troubleshooting.
#duplicated = merged_df[(merged_df['duplicate_players'] == True) & \
#                       (merged_df['active_in_season'] == True)]


# FOR COACH -- add the primary key. Do this for all of the columns needed, then load it into a staging table
# in SQL Server. Lookup fks on the staging table then load to the fact table.


df_size['After Player Dup Removal'] = len(merged_df)
sized = pd.DataFrame.from_dict(df_size, orient='index')

# Remove values that are outside of the range of 1996 - 2017
merged_df.drop(merged_df[merged_df['seasonStartYear'] < 1996].index, \
               inplace=True)
merged_df.drop(merged_df[merged_df['seasonStartYear'] > 2017].index, \
               inplace=True)

df_size['After Out of Bound Range Removal'] = len(merged_df)
sized = pd.DataFrame.from_dict(df_size, orient='index')

# Select the columns to keep
final_columns =['head_coach_dim_syn_key','player_dim_syn_key', 'team_dim_syn_key',
                  'season_date_dim_syn_key','game_dim_syn_key',
                   'PTS', 'win','FG', 'FGA','3P', '3PA', 'FT', 'FTA']

# Write merged_info to original df
df = merged_df[final_columns]

# remove non-integer values from df
df = df.replace(regex=r'\D', value='0')



df_size['After removing out of bounds characters'] = len(df)
sized = pd.DataFrame.from_dict(df_size, orient='index')
###############################################################################
# Issue - coach DIM not getting transferred.
###############################################################################


# Remove playoff games
#df = df.dropna(subset=['head_coach_dim_syn_key','player_dim_syn_key',
#                       'team_dim_syn_key','season_date_dim_syn_key',
#                       'game_dim_syn_key'])

#df = df.astype(int)

df_size['After removing playoff games'] = len(df)
sized = pd.DataFrame.from_dict(df_size, orient='index')



# ---------------------------------------------------------------------------
# ---------------------- Write to CSV ---------------------------------------

df.to_csv(data_dir + '_scoringfact.csv')
sized.to_csv(data_dir + '_count.csv')

print('File Write Finished.')

#conn.close()
#print('Connection closed.')

#%%
