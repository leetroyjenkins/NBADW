
import os
from pythonFiles.myfunctions import *
from nba_api.stats.endpoints import CommonAllPlayers
from nba_api import *





# Use the NBA API to get a list of all players from stats.nba.com
nba_players = CommonAllPlayers().get_data_frames()
players_df = nba_players[0]

# Use the NBA API to get box scores.

#box_scores = nba_api.stats.endpoints.BoxScoreScoringV2.get_box_scores()

#%%
