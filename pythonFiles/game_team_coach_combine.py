#%%
from pythonFiles.myfunctions import *
import pandas
import os

#%% Sets directory to the root folder for the NBADW project.
set_directory_to_nbadw()

cwd = os.getcwd()
source_dir = cwd + "\\DataFiles\\Source_Data\\"
data_dir = cwd + '\\DataFiles\\'

#%% Adding Coach to Team dataframe
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