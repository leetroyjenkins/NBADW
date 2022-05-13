#%%
from pythonFiles.myfunctions import *
import pandas
import os

#%% Sets directory to the root folder for the NBADW project.
set_directory_to_nbadw()

cwd = os.getcwd()
source_dir = cwd + "\\DataFiles\\Source_Data\\"
data_dir = cwd + '\\DataFiles\\'

#%% Player ETL
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
#%%
