#%%
from pythonFiles.myfunctions import *
import pandas
import os

#%% Sets directory to the root folder for the NBADW project.
set_directory_to_nbadw()

cwd = os.getcwd()
source_dir = cwd + "\\DataFiles\\Source_Data\\"
data_dir = cwd + '\\DataFiles\\'

#%% Game ETL
game_df = pd.read_csv(source_dir + 'games.csv')

# Open the DateDim values to merge in the day in season
date_cols = ['full_date', 'day_number_in_season']
date_df = pd.read_excel(source_dir + '_DateDimCreationSpreadsheet.xlsx', \
                        sheet_name=2, usecols=date_cols)
# date_df = pd. read_excel(source_dir + '_DateDimCreationSpreadsheet.xlsx',worksheet=3, usecols=date_cols)

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

game_lookup_df = pd.merge(game_lookup_df, coach_plug, how='left', \
                          on=['seasonStartYear', 'Team'])




# Replace the NaN values with the coach name.
# df.Temp_Rating.fillna(df.Farheit, inplace=True)
game_lookup_df.coach_x.fillna(game_lookup_df.coach_y, inplace=True)

game_lookup_df['Team_game_in_season2'] = \
    game_lookup_df.groupby(['seasonStartYear', 'Team']) \
        ['game_id'].cumcount() + 1

# Remove NaNs in the game number
game_lookup_df.team_game_in_season.fillna(game_lookup_df.Team_game_in_season2, \
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

drop_col_coach = ['G_reg','W_reg', \
                  'L_reg','W/L%_reg','W > .500','Finish','G_playoff', \
                  'W_playoff','L_playoff','W/L%_playoff','Notes', \
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
#%%
