# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 19:06:52 2022

@author: ascherta

This file performs the initial downloading of data from Kaggle and the
transformation process on the files that will become the Dimension tables
in the NBADW.



This file is part of ETL Package 1 in SISS.
This file must be run before ETL Package 2 is run.
"""

from myfunctions import *
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
import kaggle

SetDirectoryToNBADW()

###############################################################################
# Deal with Directory Issues
###############################################################################

cwd = os.getcwd()
os.chdir(cwd + '\\SSISFiles')
cwd = os.getcwd()
source_dir = cwd + '\\DataFiles\\Source DataFiles\\'
data_dir = cwd + '\\DataFiles\\'

###############################################################################
# Download datasets from Kaggle
# Requires Kaggle API installed in C:\Users\[user]\.kaggle
###############################################################################

# List of datasets/files to download
download_list = ['patrickhallila1994/nba-data-from-basketball-reference',
                 'boonpalipatana/nba-season-records-from-every-year']

kaggle.api.authenticate()
print('Downloading datasets. This will take a bit.')

for i in download_list:
    kaggle.api.dataset_download_files(i, path=source_dir, unzip=True)

print('Kaggle Datasets extracted.')
###############################################################################
# Start Dimension ETL
###############################################################################




###############################################################################
# PLayer ETL
###############################################################################
player_df = pd.read_csv(source_dir + 'player_info.csv')

# Change DOB from text to a date.
player_df['birthDate'] = pd.to_datetime(player_df['birthDate'])
player_df['birthDate'] = player_df['birthDate'].dt.strftime('%m-%d-%Y')

# Change Wt from float to INT
player_df['Wt'] = player_df['Wt'].fillna(0).astype(int)

# Remove characters and deal with edge cases in names
player_df['playerName'].replace(to_replace=r'\*', \
                                value='', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' Sr.', \
                                value='_Sr.', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' Jr.', \
                                value='_Jr.', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' III', \
                                value='_III', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' II,', \
                                value='_II,', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' Del ', \
                                value=' Del_', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' Von ', \
                                value=' Von_', regex=True , inplace=True)
player_df['playerName'].replace(to_replace=r' Van ', \
                                value=' Van_', regex=True , inplace=True)

# Remove characters and deal with edge cases in names
player_df['Colleges'].replace(to_replace=r',', \
                              value='/', regex=True , inplace=True)

# First Name and Last Name Seperate by last space
namesdf = player_df['playerName'].str.rsplit(' ', 1, expand=True)
# Name columns
namesdf = namesdf.rename(columns={0:'firstName', 1:'lastName'})

# Add first and last names to player data frame
player_df = player_df.join(namesdf)

player_df.to_csv(data_dir + '_players.csv')
print('Player ETL complete.')

###############################################################################
# Team ETL
###############################################################################
# Read NBA team data.
full_df = pd.read_csv(source_dir + 'Team_Records.csv')
print(full_df.columns)

# Filter the dataframe to only keep relevant data.
df = full_df[['Season', 'Team', 'Coaches', 'Lg', 'W', 'L']]

season_start = df['Season'].str[:4]

# Check if the lengths of the season and the df are the same
print('The lengths match:',len(season_start) == len(df))

df.insert(2,'season_start_year', season_start, True)

# Loads an excel spreadsheet that I manually parsed from Wikipedia to
# include conference and division information. Transformation will be needed.
tl = pd.read_excel(source_dir + '_ConferenceandTeambyYear.xlsx')


# Transform the structure of the file so that each team is listed with
# the league, conference, and division using Pandas.melt
teamdf = tl.melt(id_vars=['pk', 'num', 'Season Start Year', 'League'])


teamdf['Index'] = teamdf['pk'].astype(str) + '-' + teamdf['variable'] + \
                  '-' + teamdf['num'].astype(str)

# remove NaNs and set the index to the pk column.
teamdf.dropna(inplace=True)
teamdf = teamdf.set_index(['Index'])

# Create division and conference columns by splitting the current 'variable'
# column that contains the conference and division in a 'conference'-'division'
# format.
teamdf[['Conference', 'Division']] = teamdf['variable'].str.split(pat='-',
                                                                  expand=True)

# Rename 'value' column to 'Team'.
teamdf = teamdf.rename(columns={'value': 'Team'})

# Re-arrange columns and save new dataframe to csv for future use.
col_order = ['Season Start Year',  'League', 'Conference',
             'Division', 'Team']

teamdf = teamdf.reindex(columns=col_order)



teamdf.sort_values(by=['Season Start Year', 'Conference', 'Division'],
                   inplace=True)

# Save to csv.
teamdf.to_csv(data_dir + '_NBAANDBBATEAMS.csv', index=True)

# This section will pull the conference and division information into
# the original dataframe (df) that contains team stats. Afterwards,
# a new file will be saved as a .csv to be imported into SQL Server
# using VS Code.

# Transform 'Team' column to remove the '*' from the end of team names.
df['Team'].replace(to_replace=r'\*', value='', regex=True , inplace=True)
teamdf['Team'].replace(to_replace=r'\*', value='', regex=True , inplace=True)

# Change to int type for merging. Without doing this one will show up as
# an object-string and the other as an object-int and they won't match
# for the merge.
df['season_start_year'] = df['season_start_year'].astype('int32')


# Issue with the Kansas City Omaha Kings. Need to remove hyphen.
# Replace Kansas City-Omaha Kings with 'Kansas City Omaha Kings'
df.loc[df['Team'] == 'Kansas City-Omaha Kings', 'Team'] = \
    'Kansas City-Omaha Kings'
teamdf.loc[teamdf['Team'] == 'Kansas City/Omaha Kings', 'Team'] = \
    'Kansas City-Omaha Kings'

newdf = pd.merge(df, teamdf, how='left', left_on=['season_start_year', 'Team']
                 , right_on=['Season Start Year', 'Team'])

# Create index for teams
newdf['Index'] = newdf['season_start_year'].astype(str) + '-' + \
                 newdf['Team']

# Filter newdf to just columns that will be loaded into the datawarehouse.
# Create list of columns
cols = ['Index', 'Team', 'season_start_year', 'League', 'Conference',
        'Division', 'Season']

# restructure newdf with only the selected columns.
newdf = newdf.reindex(columns=cols)
newdf.set_index('Index', inplace=True)
newdf = newdf.rename(columns={'season_start_year': 'Season Start Year'})
###############################################################################


# Remove teams that are from before 1996
newdf.drop(newdf[newdf['Season Start Year'] < 1996].index, inplace=True)


###############################################################################
# Write to .csv for loading in VS Project.
newdf.to_csv(data_dir + '_nbaTeamsDW.csv', index=True)
print('Team ETL complete.')
###############################################################################
# Adding Coach to Team
###############################################################################

team_df = pd.read_csv(source_dir + 'Team_Records.csv',
                      usecols=['Season', 'Lg', 'Team', 'Coaches'])

# Remove * from team names
team_df['Team'] = team_df['Team'].str.replace('[*]', '')

# Prep Coach Names and Info for processing
team_df['Coaches'] = team_df['Coaches'].str.replace(")","_")
team_df['Coaches'] = team_df['Coaches'].str.replace("(","_")
expanded = team_df['Coaches'].str.split('_', expand=True)

column_names = ['coach-1', 'coach1_games', 'coach-2', 'coach2_games',
                'coach-3', 'coach3_games', 'coach-4', 'coach4_games',
                'nothing']

# Add columns from expanded df to team_df
team_df[column_names] = expanded

# Calculate total games for each coach each season
coach1_total = team_df['coach1_games'].str.split('-', expand=True)
coach1_total['total'] = coach1_total[0].astype(int) + \
                        coach1_total[1].astype(int)

# Calculate total games for each coach each season
coach2_total = team_df['coach2_games'].str.split('-', expand=True)
coach2_total = coach2_total.fillna(0)
coach2_total['total'] = coach2_total[0].astype(int) + \
                        coach2_total[1].astype(int)

# Calculate total games for each coach each season
coach3_total = team_df['coach3_games'].str.split('-', expand=True)
coach3_total = coach3_total.fillna(0)
coach3_total['total'] = coach3_total[0].astype(int) + \
                        coach3_total[1].astype(int)

# Calculate total games for each coach each season
coach4_total = team_df['coach4_games'].str.split('-', expand=True)
coach4_total = coach4_total.fillna(0)
coach4_total['total'] = coach4_total[0].astype(int) + \
                        coach4_total[1].astype(int)

# Combine the data into a dataframe
team_df['coachtotal_1'] = coach1_total['total']
team_df['coachtotal_2'] = coach2_total['total']
team_df['coachtotal_3'] = coach3_total['total']
team_df['coachtotal_4'] = coach4_total['total']

# Select columns to keep in the dataframe
column_order = ['Season', 'Team', 'coach-1', 'coachtotal_1', 'coach-2',
                'coachtotal_2', 'coach-3', 'coachtotal_3', 'coach-4',
                'coachtotal_4']

team_df = team_df[column_order]

# Remove extra spaces and None values
team_df['Team'].str.strip()
team_df['coach-1'].str.strip()
team_df['coach-2'].str.strip()
team_df['coach-3'].str.strip()
team_df['coach-4'].str.strip()
team_df.fillna("")


# Melt the columns
team_df_melted = pd.melt(team_df, id_vars=['Season', 'Team'])
team_df_melted.sort_values(by=['Season', 'Team'])
# Split the variable columns so that each coach has a column with their number
holder_df = team_df_melted['variable'].str.split('-', expand=True)
team_df_melted['variable'] = holder_df[0]
team_df_melted['coach_num_in_season'] = holder_df[1]

# Split into two dataframes. One with the coach variable and the sequence
# and the other with the number of games they coached for in the season.
# split
team_df_melted_grouped = team_df_melted.groupby(team_df_melted.variable)
melted_coach = team_df_melted_grouped.get_group('coach')
melted_values = team_df_melted[team_df_melted['variable'] != 'coach']

# Create a value that will match to the coach sequence in season value in the
# coach dataframe.
value_holder_df = melted_values['variable'].str.split('_', expand=True)
melted_values['coach_num_in_season'] = value_holder_df[1]

# merge
teamcoach_df = pd.merge(melted_coach, melted_values, how='left',
                        on=['Season', 'Team', 'coach_num_in_season'])

teamcoach_df = teamcoach_df.drop(['variable_x', 'variable_y'], axis=1)

new_col_names = ['Season', 'Team', 'coach', 'coach_num_in_season',
                 'games_coached_in_season']

teamcoach_df = teamcoach_df.rename(columns={'value_x':'coach',
                                            'value_y': 'games_coached_in_season'})
teamcoach_df['Season_Year_Begin'] = teamcoach_df['Season'].str[:4].astype(int)

teamcoach_df = teamcoach_df.sort_values(by=['Season', 'Team'])

teamcoach_df.dropna(axis=0, inplace=True)

teamcoach_df.drop(axis=0, index=teamcoach_df.index \
    [teamcoach_df['games_coached_in_season'] == 0], inplace=True)
teamcoach_df.reset_index(drop=True, inplace=True)

# Replicate the rows


# Number the ordered rows
# Add records for the number of games each coach coached during the season
teamcoach_df = teamcoach_df.loc[teamcoach_df.index.repeat \
    (teamcoach_df['games_coached_in_season'])]
teamcoach_df['coach_game_in_season'] = \
    teamcoach_df.groupby(['Season', 'Team', 'coach']).cumcount() + 1
teamcoach_df['team_game_in_season'] = \
    teamcoach_df.groupby(['Season', 'Team']).cumcount() + 1


# game_df['hometeam_game_in_season'] = \
#    game_df.groupby([
#        'seasonStartYear', 'homeTeam'])['game_id'].cumcount() + 1







# Write to CSV
teamcoach_df.to_csv(data_dir + '_teamswithcoaches.csv')

#### Clean up namespace
del melted_values
del melted_coach
del team_df_melted
del team_df_melted_grouped
del expanded
del holder_df
del value_holder_df
del coach1_total
del coach2_total
del coach3_total
del coach4_total
print('Adding Coach ETL complete.')
###############################################################################
#                          Create Game Count in Season
###############################################################################
# Update the game df with the game count for each team/season.


# Open the game dataframe
game_df = pd.read_csv(source_dir + 'games.csv')

# Open the DateDim values to merge in the day in season
date_cols = ['full_date', 'day_number_in_season']
date_df = pd.read_csv(data_dir + '_DateDim.csv', usecols=date_cols)
#date_df = pd. read_excel(source_dir + '_DateDimCreationSpreadsheet.xlsx',worksheet=3, usecols=date_cols)

# Add day in season to the game_df
game_df = pd.merge(game_df, date_df, how='left', left_on='datetime',
                   right_on='full_date')

# Drop unused columns.
game_df = game_df.drop(columns=['notes', 'startET'])

# Write to CSV for loading into game DIm then continue ETL for coach and Team
game_df.to_csv(data_dir + '_games.csv')

# Add a count of the games in each season for each team
game_df = game_df.sort_values(by=['seasonStartYear',
                                  'homeTeam', 'day_number_in_season'])

# This function groups by the team and season and counts up for each new game
# id
game_df['hometeam_game_in_season'] = \
    game_df.groupby([
        'seasonStartYear', 'homeTeam'])['game_id'].cumcount() + 1

game_df['awayteam_game_in_season'] = \
    game_df.groupby([
        'seasonStartYear', 'awayTeam'])['game_id'].cumcount() + 1

game_df.sort_values(by=['seasonStartYear','homeTeam', 'day_number_in_season'])

# Create two new dataframes, each containing the team name, game id, and
# the game in season. Then do a UNION equivalent to create a lookup table.
game_df_hometeam = game_df[['seasonStartYear', 'homeTeam', 'full_date',
                            'game_id', 'isRegular', 'hometeam_game_in_season',
                            'day_number_in_season']]
game_df_hometeam = game_df_hometeam.assign(home_or_away = 'h')
game_df_hometeam.rename(columns={'homeTeam': 'Team'}, inplace=True)

game_df_awayteam = game_df[['seasonStartYear', 'awayTeam', 'full_date',
                            'game_id', 'isRegular', 'awayteam_game_in_season',
                            'day_number_in_season']]
game_df_awayteam = game_df_awayteam.assign(home_or_away = 'a')
game_df_awayteam.rename(columns={'awayTeam': 'Team'}, inplace=True)

# Combine the two dataframes
game_lookup_df = pd.concat([game_df_awayteam, game_df_hometeam])

# Sort
game_lookup_df = game_lookup_df.sort_values(by=['seasonStartYear',
                                                'Team',
                                                'day_number_in_season'])

# This function groups by the team and season and counts up for each new game
# id
game_lookup_df['Team_game_in_season'] = \
    game_lookup_df.groupby(['seasonStartYear', 'Team']) \
        ['game_id'].cumcount() + 1

# Add coach data for


print('Game ETL complete.')
##########################################################################
#                           Game ETL - Add COACH
##########################################################################
# Add the Coach to each game.
# Coach names are in the game_df dataframe
# Fill in NaNs with the name of the coach that is the first game of the season.

game_lookup_df = pd.merge(game_lookup_df, teamcoach_df, how='left',
                          left_on=['seasonStartYear', 'Team',
                                   'Team_game_in_season'],
                          right_on=['Season_Year_Begin', 'Team',
                                    'team_game_in_season'])

# Remove unnecessary columns
dropcol = ['awayteam_game_in_season','day_number_in_season', 'home_or_away' ,
           'hometeam_game_in_season' ,
           'Team_game_in_season' , 'Season' , 'coach_num_in_season' ,
           'games_coached_in_season', 'Season_Year_Begin' ,
           'coach_game_in_season', 'full_date']

# Replace NaN fields for coach data that isn't available. Use the coach that
# coached the first game of the season.
# First make a data frame of each team/year where the game season = 1
coach_plug = game_lookup_df[game_lookup_df['Team_game_in_season'] == 1]
coach_plug = coach_plug[['Team', 'coach', 'seasonStartYear']]

game_lookup_df.drop(columns=dropcol, inplace=True)

game_lookup_df = pd.merge(game_lookup_df, coach_plug, how='left',\
                          on=['seasonStartYear', 'Team'])




# Replace the NaN values with the coach name.
# df.Temp_Rating.fillna(df.Farheit, inplace=True)
game_lookup_df.coach_x.fillna(game_lookup_df.coach_y, inplace=True)

game_lookup_df['Team_game_in_season2'] = \
    game_lookup_df.groupby(['seasonStartYear', 'Team']) \
        ['game_id'].cumcount() + 1

# Remove NaNs in the game number
game_lookup_df.team_game_in_season.fillna(game_lookup_df.Team_game_in_season2,\
                                          inplace = True)

# Remove extra columns and rename coach_x to coach.
game_lookup_df.drop(columns='coach_y', inplace=True)
game_lookup_df.rename(columns={'coach_x': 'coach'}, inplace=True)


# Remove initials. Coach can be looked up by season start year,
# # team and last name.

# Create last name column

# Break name apart on initials.
game_lookup_df['coach_last_name'] = game_lookup_df['coach'].str[2:]
# remove .
game_lookup_df['coach_last_name'] = \
    game_lookup_df['coach_last_name'].str.replace('.', '')
# Strip white spaces
game_lookup_df['coach_last_name'] = \
    game_lookup_df['coach_last_name'].str.strip()

# Replace 'St' with 'St.' for an edge case.
game_lookup_df['coach_last_name'] = \
    game_lookup_df['coach_last_name'].str.replace('St ', 'St. ')


# Add

print('Game and Coach Merge complete.')
###############################################################################
#                                    Coach.csv Changes
###############################################################################
coach_df = pd.read_csv(source_dir + 'coaches.csv')

drop_col_coach = ['G_reg','W_reg',\
                  'L_reg','W/L%_reg','W > .500','Finish','G_playoff',\
                  'W_playoff','L_playoff','W/L%_playoff','Notes',\
                  'coachType']


coach_df.drop(columns=drop_col_coach, inplace=True)

coach_name = coach_df['coachName'].str.split(' ', expand=True)[1]

coach_name.replace(to_replace='Van', value='Van Gundy', inplace=True)
coach_name.replace(to_replace='St. ', value='St. Jean', inplace=True)


coach_df['last_name'] = coach_name

# add team name from '_teamNameLookup.csv'

team_lookup = pd.read_csv(source_dir + '_teamNameLookup.csv')

coach_df = pd.merge(coach_df, team_lookup, how='left', left_on='Tm', \
                 right_on='Team Initial')

# newdf.drop(newdf[newdf['Season Start Year'] < 1996].index, inplace=True)
coach_df.drop(coach_df[coach_df['seasonStartYear'] >= 2017].index, \
              inplace=True)

coach_df.to_csv(data_dir + '_coaches.csv')




###############################################################################
#                                    Write to csv
###############################################################################
# Write to CSV
print('Writing to:', data_dir + '_game_lookup.csv')
game_lookup_df.to_csv(data_dir + '_game_lookup.csv')
print('Dimension ETL Complete.')

