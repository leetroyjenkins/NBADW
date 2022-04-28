"""
This file contains the functions for extracting data from Kaggle and the other
sources needed for the NBA DW.
"""


import os
import nba_api.stats.endpoints
import pandas as pd
from pythonFiles.myfunctions import *
from nba_api.stats.endpoints import CommonAllPlayers
import requests



# Sets directory to the root folder for the NBADW project.
set_directory_to_nbadw()

cwd = os.getcwd()
source_dir = cwd + "\\DataFiles\\Source_Data\\"
data_dir = cwd + '\\DataFiles\\'

###############################################################################
# Download datasets from Kaggle
# Requires Kaggle API installed in C:\Users\[user]\.kaggle
###############################################################################

# List of datasets/files to download
#download_list = ['patrickhallila1994/nba-data-from-basketball-reference',
#                 'boonpalipatana/nba-season-records-from-every-year']

# Download the datasets from Kaggle.
#kaggle_download_and_unzip(dataset_list=download_list, save_path=source_dir)


# Use the NBA API to get a list of all players from stats.nba.com
nba_players = CommonAllPlayers().get_data_frames()

#nba_players = nba_players2['']